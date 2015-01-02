Elasticsearch For Beginners: Indexing your GMail inbox
=======================


#### What's this all about? 

I recently looked at my GMail inbox and noticed that I have well over 50k emails, taking up about 12GB of space but there is no good way to tell what emails take up space, who sent them to, who emails me, etc

Goal of this tutorial is to load an entire GMail inbox into Elasticsearch using bulk indexing and then start querying the cluster to get a better picture of what's going on.



#### Prerequisites

Set up [Elasticsearch](http://ohardt.us/es-install) and make sure it's running at [http://localhost:9200](http://localhost:9200)

I use Python and [Tornado](https://github.com/tornadoweb/tornado/) for the scripts to import and query the data.



#### Aight, where do we start? 

First, go [here](http://ohardt.us/download-gmail-mailbox) and download your GMail mailbox, depending on the amount of emails you have accumulated this might take a while.

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

The full Python code here: [src/update.py](src/index_emails.py)


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

The most simple aproach is a PUT request per item:

```python
def upload_item_to_es(item):
    es_url = "http://localhost:9200/gmail/email/%s" % (item['message-id'])
    request = HTTPRequest(es_url, method="PUT", body=json.dumps(item), request_timeout=10)
    response = yield http_client.fetch(request)
    if not response.code in [200, 201]:
        print "\nfailed to add item %s" % item['message-id']
    
```

However, Elasticsearch provides a better method for importing large chunks of data: [bulk indexing](http://ohardt.us/es-bulk-indexing)
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

After indexing all your emails we can start running queries.

```
curl -XGET 'http://localhost:9200/gmail/email/_search?pretty&search_type=count' -d '{
"aggs": { "emails": { "terms" : { "field" : "to",  "size": 10 }
} } }
'
```

Emails, grouped by recipient:

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
  }
```

Another one:

```
curl -XGET 'http://localhost:9200/gmail/email/_search?pretty&search_type=count' -d '{
"aggs": { "labels": { "terms" : { "field" : "labels",  "size": 10 }
} } }
'
```

How many emails we have per label

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
  }
```




#### Todo

- more interesting queries
- schema tweaks
- multi-part message parsing
- ...



#### Feedback

Open pull requests, issues or email me at o@21zoo.com
