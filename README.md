Elasticsearch For Beginners: Indexing your Gmail Inbox
=======================



#### What's this all about?

I recently looked at my Gmail inbox and noticed that I have well over 50k emails, taking up about 12GB of space but there is no good way to tell what emails take up space, who sent them to, who emails me, etc

Goal of this tutorial is to load an entire Gmail inbox into Elasticsearch using bulk indexing and then start querying the cluster to get a better picture of what's going on.

__Related tutorial:__ [Index and Search Hacker News using Elasticsearch and the HN API](https://github.com/oliver006/elasticsearch-hn)


#### Prerequisites

Set up [Elasticsearch](https://www.elastic.co/guide/en/elasticsearch/guide/current/running-elasticsearch.html) and make sure it's running at [http://localhost:9200](http://localhost:9200)

I use Python and [Tornado](https://github.com/tornadoweb/tornado/) for the scripts to import and query the data. Also `beautifulsoup4` for the stripping HTML/JS/CSS (if you want to use the body indexing flag).

Install the dependencies by running:

`pip install -r requirements.txt`


#### Aight, where do we start?

First, go [here](https://www.google.com/settings/takeout/custom/gmail) and download your Gmail mailbox, depending on the amount of emails you have accumulated this might take a while.

The downloaded archive is in the [mbox format](http://en.wikipedia.org/wiki/Mbox) and Python provides libraries to work with the mbox format so that's easy.

The overall program will look something like this:

```python
mbox = mailbox.UnixMailbox(open('emails.mbox', 'rb'), email.message_from_file)

for msg in mbox:
    item = convert_msg_to_json(msg)
	upload_item_to_es(item)

print "Done!"
```

#### Ok, tell me more about the details

The full Python code is here: [src/index_emails.py](src/index_emails.py)


##### Turn mbox into JSON

First, we got to turn the mbox format messages into JSON so we can insert it into Elasticsearch. [Here](http://nbviewer.ipython.org/github/furukama/Mining-the-Social-Web-2nd-Edition/blob/master/ipynb/Chapter%206%20-%20Mining%20Mailboxes.ipynb) is some sample code that was very useful when it came to normalizing and cleaning up the data.

A good first step:

```python
def convert_msg_to_json(msg):
    result = {'parts': []}
    for (k, v) in msg.items():
        result[k.lower()] = v.decode('utf-8', 'ignore')

```

Additionally, you also want to parse and normalize the `From` and `To` email addresses:

```python
for k in ['to', 'cc', 'bcc']:
    if not result.get(k):
        continue
    emails_split = result[k].replace('\n', '').replace('\t', '').replace('\r', '').replace(' ', '').encode('utf8').decode('utf-8', 'ignore').split(',')
    result[k] = [ normalize_email(e) for e in emails_split]

if "from" in result:
    result['from'] = normalize_email(result['from'])
```

Elasticsearch expects timestamps to be in microseconds so let's convert the date accordingly

```python
if "date" in result:
    tt = email.utils.parsedate_tz(result['date'])
    result['date_ts'] = int(calendar.timegm(tt) - tt[9]) * 1000
```

We also need to split up and normalize the labels

```python
labels = []
if "x-gmail-labels" in result:
    labels = [l.strip().lower() for l in result["x-gmail-labels"].split(',')]
    del result["x-gmail-labels"]
result['labels'] = labels
```

Email size is also interesting so let's break that out

```python
parts = json_msg.get("parts", [])
json_msg['content_size_total'] = 0
for part in parts:
    json_msg['content_size_total'] += len(part.get('content', ""))

```


##### Index the data with Elasticsearch

The most simple approach is a PUT request per item:

```python
def upload_item_to_es(item):
    es_url = "http://localhost:9200/gmail/email/%s" % (item['message-id'])
    request = HTTPRequest(es_url, method="PUT", body=json.dumps(item), request_timeout=10)
    response = yield http_client.fetch(request)
    if not response.code in [200, 201]:
        print "\nfailed to add item %s" % item['message-id']

```

However, Elasticsearch provides a better method for importing large chunks of data: [bulk indexing](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html)
Instead of making a HTTP request per document and indexing individually, we batch them in chunks of eg. 1000 documents and then index them.<br>
Bulk messages are of the format:

```
cmd\n
doc\n
cmd\n
doc\n
...
```

where `cmd` is the control message for each `doc` we want to index.
For our example, `cmd` would look like this:

```
cmd = {'index': {'_index': 'gmail', '_type': 'email', '_id': item['message-id']}}`
```

The final code looks something like this:

```python
upload_data = list()
for msg in mbox:
    item = convert_msg_to_json(msg)
    upload_data.append(item)
    if len(upload_data) == 100:
        upload_batch(upload_data)
        upload_data = list()

if upload_data:
    upload_batch(upload_data)

```
and

```python
def upload_batch(upload_data):

    upload_data_txt = ""
    for item in upload_data:
        cmd = {'index': {'_index': 'gmail', '_type': 'email', '_id': item['message-id']}}
        upload_data_txt += json.dumps(cmd) + "\n"
        upload_data_txt += json.dumps(item) + "\n"

    request = HTTPRequest("http://localhost:9200/_bulk", method="POST", body=upload_data_txt, request_timeout=240)
    response = http_client.fetch(request)
    result = json.loads(response.body)
	if 'errors' in result:
	    print result['errors']
```



#### Ok, show me some data!

After indexing all your emails, we can start running queries.


##### Filters

If you want to search for emails from the last 6 months, you can use the range filter and search for `gte` the current time (`now`) minus 6 month:

```
curl -XGET 'http://localhost:9200/gmail/email/_search?pretty' -d '{
"filter": { "range" : { "date_ts" : { "gte": "now-6M" } } } }
'
```

or you can filter for all emails from 2014 by using `gte` and `lt`

```
curl -XGET 'http://localhost:9200/gmail/email/_search?pretty' -d '{
"filter": { "range" : { "date_ts" : { "gte": "2013-01-01T00:00:00.000Z", "lt": "2014-01-01T00:00:00.000Z" } } } }
'
```

You can also quickly query for certain fields via the `q` parameter. This example shows you all your Amazon shipping info emails:

```
curl "localhost:9200/gmail/email/_search?pretty&q=from:ship-confirm@amazon.com"
```

##### Aggregation queries

Aggregation queries let us bucket data by a given key and count the number of messages per bucket.
For example, number of messages grouped by recipient:

```
curl -XGET 'http://localhost:9200/gmail/email/_search?pretty&search_type=count' -d '{
"aggs": { "emails": { "terms" : { "field" : "to",  "size": 10 }
} } }
'
```

Result:

```
"aggregations" : {
"emails" : {
  "buckets" : [ {
       "key" : "noreply@github.com",
       "doc_count" : 1920
  }, { "key" : "oliver@gmail.com",
       "doc_count" : 1326
  }, { "key" : "michael@gmail.com",
       "doc_count" : 263
  }, { "key" : "david@gmail.com",
       "doc_count" : 232
  }
  ...
  ]
}
```

This one gives us the number of emails per label:

```
curl -XGET 'http://localhost:9200/gmail/email/_search?pretty&search_type=count' -d '{
"aggs": { "labels": { "terms" : { "field" : "labels",  "size": 10 }
} } }
'
```

Result:

```
"hits" : {
  "total" : 51794,
},
"aggregations" : {
"labels" : {
  "buckets" : [       {
       "key" : "important",
       "doc_count" : 15430
  }, { "key" : "github",
       "doc_count" : 4928
  }, { "key" : "sent",
       "doc_count" : 4285
  }, { "key" : "unread",
       "doc_count" : 510
  },
  ...
   ]
}
```

Use a `date histogram` you can also count how many emails you sent and received per year:

```
curl -s "localhost:9200/gmail/email/_search?pretty&search_type=count" -d '
{ "aggs": {
    "years": {
      "date_histogram": {
        "field": "date_ts", "interval": "year"
}}}}
'
```

Result:

```
"aggregations" : {
"years" : {
  "buckets" : [ {
    "key_as_string" : "2004-01-01T00:00:00.000Z",
    "key" : 1072915200000,
    "doc_count" : 585
  }, {
...
  }, {
    "key_as_string" : "2013-01-01T00:00:00.000Z",
    "key" : 1356998400000,
    "doc_count" : 12832
  }, {
    "key_as_string" : "2014-01-01T00:00:00.000Z",
    "key" : 1388534400000,
    "doc_count" : 7283
  } ]
}
```

Write aggregation queries to work out how much you spent on Amazon/Steam:

```
GET _search
{
  "query": {
    "match_all": {}
      },
      "size": 0,
      "aggs": {
        "group_by_company": {
          "terms": {
            "field": "order_details.merchant"
            },
            "aggs": {
              "total_spent": {
                "sum": {
                  "field": "order_details.order_total"
                }
                },
                "postage": {
                  "sum": {
                    "field": "order_details.postage"
                  }
                }
              }
            }
          }
        }
```


#### Todo

- more interesting queries
- schema tweaks
- multi-part message parsing
- blurb about performance
- ...



#### Feedback

Open pull requests, issues or email me at o@21zoo.com
