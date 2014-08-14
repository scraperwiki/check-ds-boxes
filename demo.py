import datetime
from pymongo import Connection

with open("/home/secret") as f:
    conn_url = f.read().strip()
d = Connection(conn_url).get_default_database()

yesterday = datetime.datetime.now()-datetime.timedelta(hours=24)


query = {"status.type": "error",
         "boxServer": "ds-ec2.scraperwiki.com",
         "status.message": {"$ne": "Permission needed from Twitter"},
         "status.updated": {"$gt": yesterday}}

response = list(d.datasets.find(query).limit(10))

most_recent = max(response, key=lambda x: x['status']['updated'])['status']['updated']
print most_recent
for x in response:
    print x['displayName'], x['status']['message'], x['box']
