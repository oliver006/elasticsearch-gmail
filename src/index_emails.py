from tornado.httpclient import HTTPClient, HTTPRequest
from tornado.ioloop import IOLoop
import tornado.options
import json
import time
import calendar
import email.utils
import mailbox
import email
import quopri
import chardet
from DelegatingEmailParser import DelegatingEmailParser
from AmazonEmailParser import AmazonEmailParser
from SteamEmailParser import SteamEmailParser
from bs4 import BeautifulSoup
import logging

http_client = HTTPClient()

DEFAULT_BATCH_SIZE = 500
DEFAULT_ES_URL = "http://localhost:9200"
DEFAULT_INDEX_NAME = "gmail"


def strip_html_css_js(msg):
    soup = BeautifulSoup(msg, "html.parser")  # create a new bs4 object from the html data loaded
    for script in soup(["script", "style"]):  # remove all javascript and stylesheet code
        script.extract()
    # get text
    text = soup.get_text()
    # break into lines and remove leading and trailing space on each
    lines = (line.strip() for line in text.splitlines())
    # break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    # drop blank lines
    text = '\n'.join(chunk for chunk in chunks if chunk)
    return text


def delete_index():
    try:
        url = "%s/%s?refresh=true" % (tornado.options.options.es_url, tornado.options.options.index_name)
        request = HTTPRequest(url, method="DELETE", request_timeout=240, headers={"Content-Type": "application/json"})
        body = {"refresh": True}
        response = http_client.fetch(request)
        logging.info('Delete index done   %s' % response.body)
    except:
        pass


def create_index():

    schema = {
        "settings": {
            "number_of_shards": tornado.options.options.num_of_shards,
            "number_of_replicas": 0
        },
        "mappings": {
            "email": {
                "_source": {"enabled": True},
                "properties": {
                    "from": {"type": "string", "index": "not_analyzed"},
                    "return-path": {"type": "string", "index": "not_analyzed"},
                    "delivered-to": {"type": "string", "index": "not_analyzed"},
                    "message-id": {"type": "string", "index": "not_analyzed"},
                    "to": {"type": "string", "index": "not_analyzed"},
                    "date_ts": {"type": "date"},
                },
            }
        },
        "refresh": True
    }

    body = json.dumps(schema)
    url = "%s/%s" % (tornado.options.options.es_url, tornado.options.options.index_name)
    try:
        request = HTTPRequest(url, method="PUT", body=body, request_timeout=240, headers={"Content-Type": "application/json"})
        response = http_client.fetch(request)
        logging.info('Create index done   %s' % response.body)
    except:
        pass


total_uploaded = 0


def upload_batch(upload_data):
    upload_data_txt = ""
    for item in upload_data:
        cmd = {'index': {'_index': tornado.options.options.index_name, '_type': 'email', '_id': item['message-id']}}
        upload_data_txt += json.dumps(cmd) + "\n"
        upload_data_txt += json.dumps(item) + "\n"

    request = HTTPRequest(tornado.options.options.es_url + "/_bulk", method="POST", body=upload_data_txt, request_timeout=240, headers={"Content-Type": "application/json"})
    response = http_client.fetch(request)
    result = json.loads(response.body)

    global total_uploaded
    total_uploaded += len(upload_data)
    res_txt = "OK" if not result['errors'] else "FAILED"
    logging.info("Upload: %s - upload took: %4dms, total messages uploaded: %6d" % (res_txt, result['took'], total_uploaded))


def normalize_email(email_in):
    parsed = email.utils.parseaddr(email_in)
    return parsed[1]


def convert_msg_to_json(msg):
    result = {'parts': []}
    if 'message-id' not in msg:
        return None

    for (k, v) in msg.items():
        result[k.lower()] = v

    for k in ['to', 'cc', 'bcc']:
        if not result.get(k):
            continue
        emails_split = result[k].replace('\n', '').replace('\t', '').replace('\r', '').replace(' ', '').encode('utf8').decode('utf-8', 'ignore').split(',')
        result[k] = [normalize_email(e) for e in emails_split]

    if "from" in result:
        result['from'] = normalize_email(result['from'])

    if "date" in result:
        try:
            tt = email.utils.parsedate_tz(result['date'])
            tz = tt[9] if len(tt) == 10 and tt[9] else 0
            result['date_ts'] = int(calendar.timegm(tt) - tz) * 1000
        except:
            return None

    labels = []
    if "x-gmail-labels" in result:
        labels = [l.strip().lower() for l in result["x-gmail-labels"].split(',')]
        del result["x-gmail-labels"]
    result['labels'] = labels

    # Bodies...
    if tornado.options.options.index_bodies:
        result['body'] = ''
        if msg.is_multipart():
            for mpart in msg.get_payload():
                if mpart is not None:
                    mpart_payload = mpart.get_payload(decode=True)
                    if mpart_payload is not None:
                        result['body'] += strip_html_css_js(mpart_payload)
        else:
            result['body'] = strip_html_css_js(msg.get_payload(decode=True))

        result['body_size'] = len(result['body'])

    parts = result.get("parts", [])
    result['content_size_total'] = 0
    for part in parts:
        result['content_size_total'] += len(part.get('content', ""))

    return result


def load_from_file():

    if tornado.options.options.init:
        delete_index()
    create_index()

    if tornado.options.options.skip:
        logging.info("Skipping first %d messages from mbox file" % tornado.options.options.skip)

    count = 0
    upload_data = list()
    logging.info("Starting import from file %s" % tornado.options.options.infile)
    # mbox = mailbox.UnixMailbox(open(tornado.options.options.infile, 'rb'), email.message_from_file)

    # removed the above UnixMailbox which is not supported in python 3.x and replaced it with mailbox.mbox class
    mbox = mailbox.mbox(tornado.options.options.infile)

    emailParser = DelegatingEmailParser([AmazonEmailParser(), SteamEmailParser()])

    for msg in mbox:
        count += 1
        if count < tornado.options.options.skip:
            continue
        item = convert_msg_to_json(msg)

        if item:
            upload_data.append(item)
            if len(upload_data) == tornado.options.options.batch_size:
                upload_batch(upload_data)
                upload_data = list()

    # upload remaining items in `upload_batch`
    if upload_data:
        upload_batch(upload_data)

    logging.info("Import done - total count %d" % count)


if __name__ == '__main__':

    tornado.options.define("es_url", type=str, default=DEFAULT_ES_URL,
                           help="URL of your Elasticsearch node")

    tornado.options.define("index_name", type=str, default=DEFAULT_INDEX_NAME,
                           help="Name of the index to store your messages")

    tornado.options.define("infile", type=str, default=None,
                           help="The mbox input file")

    tornado.options.define("init", type=bool, default=False,
                           help="Force deleting and re-initializing the Elasticsearch index")

    tornado.options.define("batch_size", type=int, default=DEFAULT_BATCH_SIZE,
                           help="Elasticsearch bulk index batch size")

    tornado.options.define("skip", type=int, default=0,
                           help="Number of messages to skip from the mbox file")

    tornado.options.define("num_of_shards", type=int, default=2,
                           help="Number of shards for ES index")

    tornado.options.define("index_bodies", type=bool, default=True,
                           help="Will index all body content, stripped of HTML/CSS/JS etc. Adds fields: 'body' and \
                                    'body_size'")

    tornado.options.parse_command_line()

    if tornado.options.options.infile:
        IOLoop.instance().run_sync(load_from_file)
    else:
        tornado.options.print_help()
