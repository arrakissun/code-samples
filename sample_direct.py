import json
import math
from decimal import Decimal, ROUND_DOWN
from sqlalchemy import Column, Integer, Boolean, String, orm
import requests
from requests.exceptions import RequestException
import arrow
from leadok import app, db
from leadok.common import handle_exception

logger = app.logger

API_V5_BASE_URL = 'https://api.direct.yandex.com/json/v5/'
API_V4_LIVE_URL = 'https://api.direct.yandex.ru/live/v4/json/'
YANDEX_OAUTH_URL = 'https://oauth.yandex.ru/'
# Application identifier
CLIENT_ID = '?'
# Application password
CLIENT_SECRET = '?'
OAUTH_TOKEN = '?'


class YandexDirectAPIError(RuntimeError):
    pass


class YandexOAuthError(RuntimeError):
    pass


class Campaign(db.Model):
    __tablename__ = 'direct_campaigns'

    id = Column('campaign_id', Integer, primary_key=True)
    chosen = Column(Boolean)
    domain = Column(String)

    @orm.reconstructor
    def init_on_load(self):
        self.update_data_from_server({})

    def update_data_from_server(self, data):
        self.name = data.get('Name')
        self.state = data.get('State')
        self.status = data.get('Status')

    @property
    def on(self):
        return self.state == 'ON'

    def __repr__(self):
        return 'Campaign({}, {}, {})'.format(self.id, self.state, self.name)


def get_oauth_token(confirmation_code):
    url = YANDEX_OAUTH_URL + 'token'
    data = {
        'grant_type': 'authorization_code',
        'code': confirmation_code,
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
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


def _call_api_v5(page, method, params):
    url = '{0}{1}'.format(API_V5_BASE_URL, page)
    logger.debug(
        'Yandex.Direct API v5 method "{}" '
        'called at {}'.format(method, url)
    )
    data = {
        'method': method,
        'params': params,
    }
    headers = {
        'Authorization': 'Bearer ' + OAUTH_TOKEN,
        'Accept-Language': 'ru',
        'Content-Type': 'application/json; charset=utf-8',
    }
    try:
        response = requests.post(url,
                                 data=json.dumps(data),
                                 headers=headers).json()
    except RequestException as e:
        logger.error(
            'request error: {}'.format(e)
        )
        raise
    if response.get('error') is not None:
        raise YandexDirectAPIError('{}'.format(response))
    return response


def _call_api_v4(method, param):
    url = API_V4_LIVE_URL
    logger.debug(
        'Yandex.Direct API v4 method "{}" '
        'called at {}'.format(method, url)
    )
    data = {
        'method': method,
        'token': OAUTH_TOKEN,
        'locale': 'ru',
        'param': param,
    }
    try:
        response = requests.post(url, data=json.dumps(data)).json()
    except RequestException as e:
        logger.error(
            'requests error: {}'.format(e)
        )
        response = {}
    if response.get('error_code') is not None:
        logger.error(
            'Error in Yandex.Direct API v4 '
            'response: {}'.format(response)
        )
    return response


def _turn_on_campaign(campaign_id, on=True):
    method = 'resume' if on else 'suspend'
    word = 'ON' if on else 'OFF'
    params = {
        'SelectionCriteria': {
            'Ids': [campaign_id],
        },
    }
    result = _call_api_v5('campaigns', method, params)['result']
    key = 'ResumeResults' if on else 'SuspendResults'
    if result[key][0].get('Id') is not None:
        logger.info('Campaign {} successfully '
                    'turned {}'.format(campaign_id, word))
        return True
    logger.error(
        'Campaign {} was not turned {}. Result: '
        '{}'.format(campaign_id, word, result))
    return False


def _turn_on_domain(domain, on=True):
    word = 'ON' if on else 'OFF'
    logger.info('Turning {} {}...'.format(domain, word))
    affected_campaigns = 0
    failed_campaigns = []
    for c in get_campaigns():
        if not c.chosen or c.domain != domain.name:
            continue
        if (on and c.on) or (not on and not c.on):
            logger.debug('{} already {}'.format(c, word))
            continue
        affected_campaigns += 1
        if not _turn_on_campaign(c.id, on=on):
            failed_campaigns.append(c.id)
    if failed_campaigns:
        logger.error(
            'Some of Yandex.Direct campaigns with {0} were '
            'not turned {1} : {2}'.format(domain, word, failed_campaigns))
    elif affected_campaigns:
        logger.info(
            'Yandex.Direct ads with {0} turned {1}'.format(domain, word))


def _chunks(l, n):
    return [l[i:i+n] for i in range(0, len(l), n)]


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
        logger.error(
            'update_direct_expenses_cache : len(days) = {} '
            '(must not exceed {}!)'.format(len(days), METHOD_LIMIT))
        max_ids = 1
    else:
        max_ids = math.floor(METHOD_LIMIT / len(days))

    ids_range = [c.id for c in get_campaigns()]
    ids_parts = _chunks(ids_range, max_ids)

    num_api_calls = len(ids_parts)
    logger.debug(
        'There shall be {} calls to '
        '"GetSummaryStat" ...'.format(num_api_calls)
    )

    costs = {}
    zero = Decimal('0.00')
    for ids in ids_parts:
        param = {
            'CampaignIDS': ids,
            'StartDate': days[0],
            'EndDate': days[-1],
        }
        for x in _call_api_v4('GetSummaryStat', param).get('data', []):
            date = arrow.get(x['StatDate']).date()
            sum_search = Decimal(str(x['SumSearch']))
            sum_context = Decimal(str(x['SumContext']))
            costs[date] = costs.get(date, zero) + 30*(sum_search + sum_context)

    return costs


def _construct_campaign(server_data):
    campaign_id = int(server_data['Id'])
    campaign = Campaign.query.get(campaign_id)
    if campaign is None:
        logger.info('Campaign {} needed but '
                    'missing from DB'.format(campaign_id))
        campaign = Campaign(id=campaign_id, chosen=False, domain=None)
        db.session.add(campaign)
        db.session.commit()
        logger.info('{} inserted into DB'.format(campaign))
    campaign.update_data_from_server(server_data)
    return campaign


@handle_exception([])
def get_campaigns(ids=None):
    params = {
        'SelectionCriteria': {
            'States': ['ON', 'OFF', 'SUSPENDED', 'ENDED', 'CONVERTED'],
            'Statuses': ['ACCEPTED'],
        },
        'FieldNames': ['Id', 'Name', 'State', 'Status'],
    }
    if ids is not None:
        # Fetch only campaigns with specific Ids
        params['SelectionCriteria']['Ids'] = ids
    data = _call_api_v5('campaigns', 'get', params)['result']['Campaigns']
    campaigns = [_construct_campaign(c) for c in data]
    return sorted(campaigns, key=lambda x: x.name)


@handle_exception(None)
def get_campaign_by_id(campaign_id):
    return get_campaigns(ids=[campaign_id])[0]


@handle_exception(None)
def get_balance():
    # 18% is a value-added tax (VAT) in Russia
    TAX_COEFF = Decimal('1.18')
    # 30 is for converting to RUR
    RUR_CONV_COEFF = Decimal('30')
    the_id = [c for c in get_campaigns() if c.chosen][0].id
    amount = _call_api_v4('GetBalance', param=[the_id])['data'][0]['Rest']
    balance = RUR_CONV_COEFF / TAX_COEFF * Decimal(str(amount))
    return balance.quantize(Decimal('.01'), rounding=ROUND_DOWN)


@handle_exception(True)
def is_domain_off(domain):
    try:
        name = domain.name
    except AttributeError:
        # if domain is a string (deprecated interface)
        name = domain
    return not any(c.on and c.domain == name and
                   c.chosen for c in get_campaigns())


def turn_ads_on(domain):
    # Helper function
    _turn_on_domain(domain, on=True)


def turn_ads_off(domain):
    # Helper funciton
    _turn_on_domain(domain, on=False)


def update_campaign(campaign_id, chosen=False, domain=None):
    campaign = get_campaign_by_id(campaign_id)
    campaign.chosen = chosen
    campaign.domain = domain
    db.session.commit()
    logger.info('{} successfully updated'.format(campaign))
