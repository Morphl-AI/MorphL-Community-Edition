# pip install cassandra-driver

from os import getenv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

import pandas as pd

MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')
MORPHL_CASSANDRA_USERNAME = getenv('MORPHL_CASSANDRA_USERNAME')
MORPHL_CASSANDRA_PASSWORD = getenv('MORPHL_CASSANDRA_PASSWORD')
MORPHL_CASSANDRA_KEYSPACE = getenv('MORPHL_CASSANDRA_KEYSPACE')

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

auth_provider = PlainTextAuthProvider(
    username=MORPHL_CASSANDRA_USERNAME, password=MORPHL_CASSANDRA_PASSWORD)

cluster = Cluster(
    contact_points=[MORPHL_SERVER_IP_ADDRESS], auth_provider=auth_provider)

session = cluster.connect(MORPHL_CASSANDRA_KEYSPACE)
session.row_factory = pandas_factory
session.default_fetch_size = 10000000

select_statement = 'SELECT * FROM ps_area PER PARTITION LIMIT 1'
result = session.execute(select_statement)
df = result._current_rows

print(df)

