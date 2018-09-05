# pip install cassandra-driver

from os import getenv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')
MORPHL_CASSANDRA_USERNAME = getenv('MORPHL_CASSANDRA_USERNAME')
MORPHL_CASSANDRA_PASSWORD = getenv('MORPHL_CASSANDRA_PASSWORD')
MORPHL_CASSANDRA_KEYSPACE = getenv('MORPHL_CASSANDRA_KEYSPACE')

auth_provider = PlainTextAuthProvider(
    username=MORPHL_CASSANDRA_USERNAME, password=MORPHL_CASSANDRA_PASSWORD)

cluster = Cluster(
    contact_points=[MORPHL_SERVER_IP_ADDRESS], auth_provider=auth_provider)

session = cluster.connect(MORPHL_CASSANDRA_KEYSPACE)

insert_statement = 'INSERT INTO ps_area (client_id,day_of_data_capture,days_since_last_seen) VALUES (?,?,?)'
prep_stmt_for_ps_area_insert = session.prepare(insert_statement)
bind_list_for_ps_area_insert = ['GA8','2018-07-15',28]
session.execute(prep_stmt_for_ps_area_insert, bind_list_for_ps_area_insert)

