import json
import math

from sqlalchemy.exc import IntegrityError
import requests
import arrow

from leadok.common import handle_exception, log
from leadok.database import direct_campaigns as direct_campaigns_table
from leadok.database import engine
import leadok.settings


API_V5_BASE_URL = 'https://api.direct.yandex.com/json/v5/'
API_V4_LIVE_URL = 'https://api.direct.yandex.ru/live/v4/json/'
YANDEX_OAUTH_URL = 'https://oauth.yandex.ru/'


class YandexDirectAPIError(RuntimeError):
    pass


class YandexOAuthError(RuntimeError):
    pass


class Campaign:

    def __init__(self, data):
        self.id = int(data['Id'])
        self.name = data['Name']
        self.state = data['State']
        self.status = data['Status']
        self.on = (self.state == 'ON')
        self.domain = None
        self.chosen = False
        t = direct_campaigns_table
        info = engine.execute(
            t.select(t.c.campaign_id == self.id)
        ).first()
        if info is not None:
            self.domain = info['domain']
            self.chosen = info['chosen']

    def __repr__(self):
        s = "<Campaign [{0}, '{1}']>"
        return s.format(self.id, self.name)


def get_oauth_token(confirmation_code):
    url = YANDEX_OAUTH_URL + 'token'
    settings = leadok.settings.get_settings()
    client_id = settings['CLIENT_ID']
    client_secret = settings['CLIENT_SECRET']
    data = {
        'grant_type': 'authorization_code',
        'code': confirmation_code,
        'client_id': client_id,
        'client_secret': client_secret,
    }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    response = requests.post(
        url,
        data=data,
        headers=headers
    ).json()
    if 'access_token' not in response:
        raise YandexOAuthError('{}'.format(response))
    return response['access_token']


def call_api_v5(page, method, params):
    url = '{0}{1}'.format(API_V5_BASE_URL, page)
    oauth_token = leadok.settings.get_settings()['OAUTH_TOKEN']
    data = {
        'method': method,
        'params': params,
    }
    headers = {
        'Authorization': 'Bearer ' + oauth_token,
        'Accept-Language': 'ru',
        'Content-Type': 'application/json; charset=utf-8',
    }
    response = requests.post(url,
                             data=json.dumps(data),
                             headers=headers).json()
    if response.get('error') is not None:
        raise YandexDirectAPIError('{}'.format(response))
    return response


def call_api_v4(method, param):
    log('Yandex.Direct API v4 method "{}" '
        'called'.format(method), level='DEBUG')
    url = API_V4_LIVE_URL
    oauth_token = leadok.settings.get_settings()['OAUTH_TOKEN']
    data = {
        'method': method,
        'token': oauth_token,
        'locale': 'ru',
        'param': param,
    }
    response = requests.post(url, data=json.dumps(data)).json()
    error_code = response.get('error_code')
    if error_code is not None:
        raise YandexDirectAPIError('error_code: {} in Yandex.Direct API '
                                   'response'.format(error_code))
    return response


def get_campaigns(ids=None):
    params = {
        'SelectionCriteria': {
            'States': ['ON', 'OFF', 'SUSPENDED', 'ENDED', 'CONVERTED'],
            'Statuses': ['ACCEPTED'],
        },
        'FieldNames': ['Id', 'Name', 'State', 'Status'],
    }
    if ids is not None:
        params['SelectionCriteria']['Ids'] = ids
    data = call_api_v5('campaigns', 'get', params)['result']
    campaigns = [Campaign(item) for item in data.get('Campaigns', [])]
    return sorted(campaigns, key=lambda x: x.name)


def make_campaign_chosen(campaign_id, chosen, domain=None):
    t = direct_campaigns_table
    try:
        q_ins = t.insert().values(campaign_id=campaign_id,
                                  chosen=chosen,
                                  domain=domain)
        engine.execute(q_ins).close()
    except IntegrityError:
        # The exception is raised if
        # campaign already exists in table
        q_upd = t.update(t.c.campaign_id == campaign_id).\
                  values(chosen=chosen,
                         domain=domain)
        engine.execute(q_upd).close()


def get_campaign(campaign_id):
    try:
        return list(get_campaigns([campaign_id]))[0]
    except IndexError:
        # Raised if returned list is empty
        return None


def get_balance():
    # 18% is a value-added tax (VAT) in Russia
    # 30 is for converting to RUR
    TAX_COEFF = 1.18
    RUR_CONV_COEFF = 30
    try:
        the_id = [x for x in get_campaigns() if x.chosen][0].id
    except IndexError:
        raise YandexDirectAPIError('Cannot fetch balance')
    result = call_api_v4('GetBalance', param=[the_id])
    return RUR_CONV_COEFF / TAX_COEFF * result['data'][0]['Rest']


@handle_exception(False)
def turn_on_campaign(campaign_id, on=True):
    method = 'resume' if on else 'suspend'
    params = {
        'SelectionCriteria': {
            'Ids': [campaign_id],
        },
    }
    result = call_api_v5('campaigns', method, params)['result']
    key = 'ResumeResults' if on else 'SuspendResults'
    if result[key][0].get('Id') is not None:
        log('Campaign {} successfully '
            '{}ed'.format(campaign_id, method),
            level='DEBUG')
        return True
    log('Campaign {} was not {}ed. Result: '
        '{}'.format(campaign_id, method, result), level='ERROR')
    return False


@handle_exception(True)
def is_domain_off(domain):
    # Checks whether all direct campaigns
    # with domain are already turned off
    cs = [x for x in get_campaigns() if x.chosen and
          x.domain == domain.name]
    return all(not c.on for c in cs)


@handle_exception(False)
def turn_on_domain(domain, on=True):
    # Turning on/off an entire domain
    failed_ids = []
    for c in get_campaigns():
        if (not c.chosen or
                c.domain != domain.name or
                (on and c.on) or
                (not on and not c.on)):
            continue
        if not turn_on_campaign(c.id, on=on):
            failed_ids.append(c.id)
    if failed_ids:
        log('Some of the chosen Yandex.Direct campaigns '
            'with domain [{}] were not turned ON/OFF : '
            '{}'.format(domain.name, failed_ids), level='ERROR')
        return False
    return True


def _chunks(l, n):
    return [l[i:i+n] for i in range(0, len(l), n)]


@handle_exception({})
def get_direct_expenses(days_back=7):

    # You cannot use the API method if you exceed METHOD_LIMIT
    # num_campaign_ids * num_days
    METHOD_LIMIT = 1000

    direct_timezone = 'Europe/Moscow'

    start = arrow.now(direct_timezone).replace(days=-days_back)
    end = arrow.now(direct_timezone)
    days = [x.format('YYYY-MM-DD') for x in arrow.Arrow.
            range('day', start, end)]
    if len(days) >= METHOD_LIMIT:
        log('update_direct_expenses_cache : len(days) = {} '
            '(must not exceed {}!)'.format(len(days), METHOD_LIMIT),
            level='ERROR')
        max_ids = 1
    else:
        max_ids = math.floor(METHOD_LIMIT / len(days))

    ids_range = [c.id for c in get_campaigns()]
    ids_parts = _chunks(ids_range, max_ids)

    num_api_calls = len(ids_parts)
    log('There shall be {} calls to '
        '"GetSummaryStat" ...'.format(num_api_calls))

    costs = {}
    for ids in ids_parts:
        param = {
            'CampaignIDS': ids,
            'StartDate': days[0],
            'EndDate': days[-1],
        }
        for x in call_api_v4('GetSummaryStat', param)['data']:
            date = arrow.get(x['StatDate']).date()
            costs[date] = costs.get(date, 0.0) + \
                30 * (x['SumSearch'] + x['SumContext'])

    return costs
