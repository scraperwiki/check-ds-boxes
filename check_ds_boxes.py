#!/usr/bin/env python
import datetime
import pymongo
import scraperwiki
import fail_email


YESTERDAY = datetime.datetime.now()-datetime.timedelta(hours=24)
d = None

def mongo_connection(filename='/home/secret'):
    global d
    with open(filename) as f:
        conn_url = f.read().strip()
    d = pymongo.Connection(conn_url).get_default_database()
    return d

def fetch_failures_since(last_run):
    query = {"status.type": "error",
             "boxServer": "ds-ec2.scraperwiki.com",
             "status.message": {"$ne": "Permission needed from Twitter"},
             "status.updated": {"$gt": last_run}}
    return list(d.datasets.find(query))

def most_recent(data):
    if data:
        return max(response, key=lambda x: x['status']['updated'])['status']['updated']
    else:
        return None

def get_last_run_time():
    last_run_string = scraperwiki.sql.get_var('last_run')
    if last_run_string:
        return datetime.datetime.strptime(last_run_string, "%Y-%m-%d %H:%M:%S.%f" )
    else:
        return YESTERDAY

def save_most_recent(data):
    fail_time = most_recent(data)
    if fail_time:
        scraperwiki.sql.save_var('last_run', fail_time)

def generate_text(data):
    output = []
    output.append("Hey there: the following boxes have new errors...\n\n")
    for failure in data:
        output.append("{name} ({box}): '{msg}'\n".format(name=failure['displayName'],
                                                         box=failure['box'],
                                                         msg=failure['status']['message']))
    output.append("\nCould you have a look? Thanks!\n  --ds-bot")
    return ''.join(output)

def generate_html(data):
    output = []
    output.append("Hey there: the following boxes have new errors...<br><br>\n")
    for failure in data:
        output.append("<a href='http://scraperwiki.com/dataset/{box}'>{name} (<tt><small>{box}</small></tt>)</a>: <font color=red>'{msg}'</font><br>\n".format(
                                                         name=failure['displayName'],
                                                         box=failure['box'],
                                                         msg=failure['status']['message']))
    output.append("\nCould you have a look? Thanks!<br>&nbsp;&nbsp;-- ds-bot")
    return ''.join(output)

def send_email(data):
    if len(data):
        fail_email.send_email(generate_text(data),
                              generate_html(data))
    else:
        print 'Not sending email: no rows :)'

mongo_connection()
last_run = get_last_run_time()
response = fetch_failures_since(last_run)
print generate_text(response)
send_email(response)
save_most_recent(response)
