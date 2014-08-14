from os import environ
from pymongo import Connection
with open("/home/secret") as f:
    conn_url = f.read().strip()
d = Connection(conn_url).get_default_database()
print d.users.count()
