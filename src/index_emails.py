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

    print 'Delete index done'
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
    print 'Create index done'
    time.sleep(1)

total_uploaded = 0
def upload_batch(upload_data):
    upload_data_txt = ""
    for item in upload_data:
        cmd = {'index': {'_index': 'gmail', '_type': 'email', '_id': item['message-id']}}
        upload_data_txt += json.dumps(cmd) + "\n"
        upload_data_txt += json.dumps(item) + "\n"

    request = HTTPRequest("http://localhost:9200/_bulk", method="POST", body=upload_data_txt, request_timeout=240)
    response = http_client.fetch(request)
    result = json.loads(response.body)

    global total_uploaded
    total_uploaded += len(upload_data)
    print "Errors during upload: %s -  upload took: %4dms, total messages uploaded: %6d" % (result['errors'], result['took'], total_uploaded)


def normalize_email(email_in):
    parsed = email.utils.parseaddr(email_in)
    return parsed[1]


def convert_msg_to_json(msg):
    result = {'parts': []}
    if not 'message-id' in msg:
        return False

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
        tz = tt[9] or 0
        result['date_ts'] = int(calendar.timegm(tt) - tz) * 1000

    labels = []
    if "x-gmail-labels" in result:
        labels = [l.strip().lower() for l in result["x-gmail-labels"].split(',')]
        del result["x-gmail-labels"]
    result['labels'] = labels

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
        print "Skipping first %d messages from mbox file" % tornado.options.options.skip

    count = 0
    upload_data = list()
    mbox = mailbox.UnixMailbox(open(tornado.options.options.infile, 'rb'), email.message_from_file)
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

    print "Done - total count %d" % count


if __name__ == '__main__':

    tornado.options.define(
        "infile",
        type=str,
        default=None,
        help="The mbox input file")

    tornado.options.define(
        "init",
        type=bool,
        default=False,
        help="Delete and re-initialize the Elasticsearch index")

    tornado.options.define(
        "batch_size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Elasticsearch bulk index batch size")

    tornado.options.define(
        "skip",
        type=int,
        default=0,
        help="Number of messages to skip from the mbox file")


    tornado.options.parse_command_line()

    if tornado.options.options.infile:
        IOLoop.instance().run_sync(load_from_file)
