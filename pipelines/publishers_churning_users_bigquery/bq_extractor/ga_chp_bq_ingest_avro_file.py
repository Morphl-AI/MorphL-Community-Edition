from os import getenv
from pyspark.sql import functions as f, SparkSession

MASTER_URL = 'local[*]'
APPLICATION_NAME = 'ingest_avro'

DAY_OF_DATA_CAPTURE = getenv('DAY_OF_DATA_CAPTURE')
WEBSITE_URL = getenv('WEBSITE_URL')
LOCAL_AVRO_FILE = getenv('LOCAL_AVRO_FILE')
TRAINING_OR_PREDICTION = getenv('TRAINING_OR_PREDICTION')
MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')
MORPHL_CASSANDRA_USERNAME = getenv('MORPHL_CASSANDRA_USERNAME')
MORPHL_CASSANDRA_PASSWORD = getenv('MORPHL_CASSANDRA_PASSWORD')
MORPHL_CASSANDRA_KEYSPACE = getenv('MORPHL_CASSANDRA_KEYSPACE')


def main():
    spark_session = (
        SparkSession.builder
        .appName(APPLICATION_NAME)
        .master(MASTER_URL)
        .config('spark.cassandra.connection.host', MORPHL_SERVER_IP_ADDRESS)
        .config('spark.cassandra.auth.username', MORPHL_CASSANDRA_USERNAME)
        .config('spark.cassandra.auth.password', MORPHL_CASSANDRA_PASSWORD)
        .config('spark.sql.shuffle.partitions', 16)
        .getOrCreate())

    log4j = spark_session.sparkContext._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

    avro_df = (
        spark_session
        .read
        .format('avro')
        .load(LOCAL_AVRO_FILE))

    save_options_ga_chp_bq_features_raw = {
        'keyspace': MORPHL_CASSANDRA_KEYSPACE,
        'table': 'ga_chp_bq_features_raw_t' if TRAINING_OR_PREDICTION ==
        'training' else 'ga_chp_bq_features_raw_p'
    }

    (avro_df
     .withColumn('day_of_data_capture', f.lit(DAY_OF_DATA_CAPTURE))
     .withColumn('website_url', f.lit(WEBSITE_URL))
     .write
     .format('org.apache.spark.sql.cassandra')
     .mode('append')
     .options(**save_options_ga_chp_bq_features_raw)
     .save())


if __name__ == '__main__':
    main()
