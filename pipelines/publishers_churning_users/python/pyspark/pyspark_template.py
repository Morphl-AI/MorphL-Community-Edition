# spark-submit --jars /opt/spark/jars/spark-cassandra-connector.jar python/pyspark/pyspark_template.py

from os import getenv
from pyspark.sql import SparkSession

MASTER_URL = 'local[*]'
APPLICATION_NAME = 'preprocessor'
MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')
MORPHL_CASSANDRA_USERNAME = getenv('MORPHL_CASSANDRA_USERNAME')
MORPHL_CASSANDRA_PASSWORD = getenv('MORPHL_CASSANDRA_PASSWORD')
MORPHL_CASSANDRA_KEYSPACE = getenv('MORPHL_CASSANDRA_KEYSPACE')
MORPHL_CASSANDRA_TABLE = 'ps_area'

sparkSession = SparkSession.builder \
    .appName(APPLICATION_NAME) \
    .master(MASTER_URL) \
    .config('spark.cassandra.connection.host', MORPHL_SERVER_IP_ADDRESS) \
    .config('spark.cassandra.auth.username', MORPHL_CASSANDRA_USERNAME) \
    .config('spark.cassandra.auth.password', MORPHL_CASSANDRA_PASSWORD) \
    .config('spark.sql.shuffle.partitions', 16) \
    .getOrCreate()

log4j = sparkSession.sparkContext._jvm.org.apache.log4j
log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

load_options = { 'keyspace': MORPHL_CASSANDRA_KEYSPACE,
                 'table': MORPHL_CASSANDRA_TABLE,
                 'spark.cassandra.input.fetch.size_in_rows': '150' }

ps_area_df = sparkSession.read.format('org.apache.spark.sql.cassandra') \
                              .options(**load_options) \
                              .load()

print(ps_area_df.collect())

