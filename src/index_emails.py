from tornado.httpclient import HTTPClient, HTTPRequest
from tornado.ioloop import IOLoop
import tornado.options
import json
import time
import calendar
import email.utils
import mailbox
import email


http_client = HTTPClient()

DEFAULT_BATCH_SIZE = 500
ES_URL  = "http://localhost:9200/gmail"

def delete_index():
    try:
        request = HTTPRequest(ES_URL, method="DELETE", request_timeout=240)
        response = http_client.fetch(request)
        print response.body
    except:
        pass

    print 'Delete idx done'
    time.sleep(1)


def create_index():

    schema = {
        "settings" : {
            "number_of_shards" :   2,
            "number_of_replicas" : 0
        },
        "mappings" : {
            "email" : {
                "_source" : { "enabled" : True },
                "properties" : {
                    "from" :         { "type" : "string", "index" : "not_analyzed" },
                    "return-path" :      { "type" : "string", "index" : "not_analyzed" },
                    "delivered-to" :   { "type" : "string", "index" : "not_analyzed" },
                    "message-id" : { "type" : "string", "index" : "not_analyzed" },
                    "to" :        { "type" : "string", "index" : "not_analyzed" },
                    "date_ts" :          { "type" : "date"    },
                },
            }
        }
    }

    body = json.dumps(schema)
    request = HTTPRequest(ES_URL, method="PUT", body=body, request_timeout=240)
    response = http_client.fetch(request)
    print response.body
    print 'Create idx done'
    time.sleep(1)


def upload_batch(upload_data):
    upload_data_txt = ""
    for item in upload_data:
        cmd = {'index': {'_index': 'gmail', '_type': 'email', '_id': item['message-id']}}
        upload_data_txt += json.dumps(cmd) + "\n"
        upload_data_txt += json.dumps(item) + "\n"

    request = HTTPRequest("http://localhost:9200/_bulk", method="POST", body=upload_data_txt, request_timeout=240)
    response = http_client.fetch(request)
    result = json.loads(response.body)
    print "Errors during upload: %s" % result['errors']


def normalize_email(email_in):
    parsed = email.utils.parseaddr(email_in)
    return parsed[1]


def convert_msg_to_json(msg):
    result = {'parts': []}
    for (k, v) in msg.items():
        result[k.lower()] = v.decode('utf-8', 'ignore')

    for k in ['to', 'cc', 'bcc']:
        if not result.get(k):
            continue
        emails_split = result[k].replace('\n', '').replace('\t', '').replace('\r', '').replace(' ', '').encode('utf8').decode('utf-8', 'ignore').split(',')
        result[k] = [ normalize_email(e) for e in emails_split]

    if "from" in result:
        result['from'] = normalize_email(result['from'])

    if "date" in result:
        tt = email.utils.parsedate_tz(result['date'])
        result['date_ts'] = int(calendar.timegm(tt) - tt[9]) * 1000

    labels = []
    if "x-gmail-labels" in result:
        labels = result["x-gmail-labels"].split(',')
        del result["x-gmail-labels"]
    result['labels'] = labels

    return result


def load_from_file():

    if tornado.options.options.init:
        delete_index()
        create_index()

    count = 0
    upload_data = list()
    mbox = mailbox.UnixMailbox(open(tornado.options.options.infile, 'rb'), email.message_from_file)
    for msg in mbox:
        count += 1
        item = convert_msg_to_json(msg)
        upload_data.append(item)
        if len(upload_data) == DEFAULT_BATCH_SIZE:
            upload_batch(upload_data)
            upload_data = list()

    # upload remaining items in `upload_batch`
    if upload_data:
        upload_batch(upload_data)

    print "Done - total count %d" % count


if __name__ == '__main__':

    tornado.options.define(
        "infile",
        type=str,
        default=None,
        help="infile")

    tornado.options.define(
        "init",
        type=str,
        default=None,
        help="infile")

    tornado.options.parse_command_line()

    if tornado.options.options.infile:
        IOLoop.instance().run_sync(load_from_file)
