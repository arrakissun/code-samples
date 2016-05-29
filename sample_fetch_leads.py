import imaplib

# Powerfull HTML parser
from bs4 import BeautifulSoup

from leadok.common import log, handle_exception
import leadok.customers
import leadok.distributor


MAILBOX = 'sample@sample.com'
PASSWORD = 'password'
NUM_FIELDS_IN_EMAIL = 6


@handle_exception(False)
def fetch_leads_from_mailbox():
    log('Fetching Leadia leads from {} ...'.format(MAILBOX))

    server = imaplib.IMAP4_SSL('imap.yandex.ru')
    server.login(MAILBOX, PASSWORD)
    server.select('INBOX')

    # ids contains ids of all unseen emails in INBOX folder:
    _, ids = server.search(None, 'UNSEEN')

    if not ids[0].split():
        log('No new Leadia leads to fetch')

    try:
        for id in ids[0].split():
            _, data = server.fetch(id, '(BODY.PEEK[TEXT])')
            html_email_body = data[0][1].decode('utf-8')
            soup = BeautifulSoup(html_email_body, 'html.parser')
            # Parsing email in order to extract lead fields.
            # Fields are contained inside <td> html tags
            # always in the same order.
            fields = [item.string.strip() for item in soup.find_all('td')]
            if len(fields) != NUM_FIELDS_IN_EMAIL:
                raise TypeError('The email does not contain '
                                'lead from leadia.ru')

            # Extracting fields from email contents
            lead = {}
            lead['name'] = fields[2]
            lead['phone'] = fields[3]
            lead['question'] = fields[5]
            lead['domain'] = 'jurist-msk'
            lead['source'] = 'leadia.ru'

            log('Leadia lead {0} fetched '
                'from {1}'.format(fields[0], mailbox))

            elena = leadok.customers.get_customer(uid='elena54')
            leadok.distributor.handle_incoming_lead(lead, customer=elena)

            # Make parsed email seen
            server.store(id, '+FLAGS', '\SEEN')
    finally:
        server.close()
        server.logout()


if __name__ == '__main__':
    fetch_leads_from_mailbox()
