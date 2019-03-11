import datetime
from os import getenv
from pyspark.sql import functions as f, SparkSession

MASTER_URL = 'local[*]'
APPLICATION_NAME = 'preprocessor'
DAY_AS_STR = getenv('DAY_AS_STR')
UNIQUE_HASH = getenv('UNIQUE_HASH')

TRAINING_OR_PREDICTION = getenv('TRAINING_OR_PREDICTION')
MODELS_DIR = getenv('MODELS_DIR')

MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')
MORPHL_CASSANDRA_USERNAME = getenv('MORPHL_CASSANDRA_USERNAME')
MORPHL_CASSANDRA_PASSWORD = getenv('MORPHL_CASSANDRA_PASSWORD')
MORPHL_CASSANDRA_KEYSPACE = getenv('MORPHL_CASSANDRA_KEYSPACE')

HDFS_PORT = 9000
HDFS_DIR_TRAINING = f'hdfs://{MORPHL_SERVER_IP_ADDRESS}:{HDFS_PORT}/{DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_bq_preproc_training'
HDFS_DIR_PREDICTION = f'hdfs://{MORPHL_SERVER_IP_ADDRESS}:{HDFS_PORT}/{DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_bq_preproc_prediction'

CHURN_THRESHOLD_FILE = f'{MODELS_DIR}/{DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_bq_churn_threshold.txt'


def fetch_from_cassandra(c_table_name, spark_session):
    load_options = {
        'keyspace': MORPHL_CASSANDRA_KEYSPACE,
        'table': c_table_name,
        'spark.cassandra.input.fetch.size_in_rows': '150'}

    df = (spark_session.read.format('org.apache.spark.sql.cassandra')
                            .options(**load_options)
                            .load())

    return df


def main():
    spark_session = (
        SparkSession.builder
        .appName(APPLICATION_NAME)
        .master(MASTER_URL)
        .config('spark.cassandra.connection.host', MORPHL_SERVER_IP_ADDRESS)
        .config('spark.cassandra.auth.username', MORPHL_CASSANDRA_USERNAME)
        .config('spark.cassandra.auth.password', MORPHL_CASSANDRA_PASSWORD)
        .config('spark.sql.shuffle.partitions', 16)
        .config('parquet.enable.summary-metadata', 'true')
        .getOrCreate())

    log4j = spark_session.sparkContext._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

    # All users from the database are already retained (they are filtered from the BQ SQL)
    ga_chp_bq_users = fetch_from_cassandra('ga_chp_bq_features_raw_t' if TRAINING_OR_PREDICTION ==
                                           'training' else 'ga_chp_bq_features_raw_p', spark_session)
    ga_chp_bq_users.createOrReplaceTempView('ga_chp_bq_users')

    # Using window functions: https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html
    grouped_by_client_id_before_dedup_sql_parts = [
        'SELECT',
        'client_id,',
        'SUM(bounces) OVER (PARTITION BY client_id) AS bounces,'
        'SUM(events) OVER (PARTITION BY client_id) AS events,'
        'SUM(page_views) OVER (PARTITION BY client_id) AS page_views,'
        'SUM(session_duration) OVER (PARTITION BY client_id) AS session_duration,'
        'SUM(sessions) OVER (PARTITION BY client_id) AS sessions,'
        'FIRST_VALUE(is_desktop) OVER (PARTITION BY client_id ORDER BY day_of_data_capture DESC) AS is_desktop,'
        'FIRST_VALUE(is_mobile) OVER (PARTITION BY client_id ORDER BY day_of_data_capture DESC) AS is_mobile,'
        'FIRST_VALUE(is_tablet) OVER (PARTITION BY client_id ORDER BY day_of_data_capture DESC) AS is_tablet,'
        'ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY day_of_data_capture DESC) AS rownum'
    ]

    if TRAINING_OR_PREDICTION == 'training':
        grouped_by_client_id_before_dedup_sql_parts = grouped_by_client_id_before_dedup_sql_parts + [
            ', FIRST_VALUE(days_since_last_session) OVER (PARTITION BY client_id ORDER BY day_of_data_capture DESC) AS days_since_last_session,',
            'AVG(days_since_last_session) OVER (PARTITION BY client_id) AS avgdays',
        ]

    grouped_by_client_id_before_dedup_sql_parts = grouped_by_client_id_before_dedup_sql_parts + [
        'FROM',
        'ga_chp_bq_users'
    ]

    grouped_by_client_id_before_dedup_sql = ' '.join(
        grouped_by_client_id_before_dedup_sql_parts)
    grouped_by_client_id_before_dedup_df = spark_session.sql(
        grouped_by_client_id_before_dedup_sql)
    grouped_by_client_id_before_dedup_df.createOrReplaceTempView(
        'grouped_by_client_id_before_dedup')

    # Only keeping the most recent record from every client id
    # rownum = 1 while day_of_data_capture is sorted in descending order
    grouped_by_client_id_sql = 'SELECT * FROM grouped_by_client_id_before_dedup WHERE rownum = 1'
    grouped_by_client_id_df = spark_session.sql(grouped_by_client_id_sql)
    grouped_by_client_id_df.createOrReplaceTempView('grouped_by_client_id')

    # The schema for grouped_by_client_id_df is:
    # |-- client_id: string (nullable = true)
    # |-- bounces: double (nullable = true)
    # |-- events: double (nullable = true)
    # |-- page_views: double (nullable = true)
    # |-- session_duration: double (nullable = true)
    # |-- sessions: double (nullable = true)
    # |-- is_desktop: double (nullable = true)
    # |-- is_mobile: double (nullable = true)
    # |-- is_tablet: double (nullable = true)
    # |-- days_since_last_session: float (nullable = true)
    # |-- rownum: integer (nullable = true)
    # |-- avgdays: double (nullable = true)

    if TRAINING_OR_PREDICTION == 'training':
        mean_value_of_avg_days_sql = 'SELECT AVG(avgdays) mean_value_of_avgdays FROM grouped_by_client_id'
        mean_value_of_avg_days_df = spark_session.sql(
            mean_value_of_avg_days_sql)
        churn_threshold = mean_value_of_avg_days_df.first().mean_value_of_avgdays

        final_df = (
            grouped_by_client_id_df
            .withColumn('churned', f.when(
                f.col('days_since_last_session') > churn_threshold, 1.0).otherwise(0.0))
            .select('client_id',
                    'bounces', 'events', 'page_views',
                    'session_duration', 'sessions',
                    'is_desktop', 'is_mobile', 'is_tablet',
                    'churned')
            .repartition(32))

        # The schema for final_df is:
        # |-- client_id: string (nullable = true)
        # |-- bounces: double (nullable = true)
        # |-- events: double (nullable = true)
        # |-- page_views: double (nullable = true)
        # |-- session_duration: double (nullable = true)
        # |-- sessions: double (nullable = true)
        # |-- is_desktop: double (nullable = true)
        # |-- is_mobile: double (nullable = true)
        # |-- is_tablet: double (nullable = true)
        # |-- churned: double (nullable = false)

        final_df.cache()

        final_df.write.parquet(HDFS_DIR_TRAINING)

        save_options_ga_chp_bq_features_training = {
            'keyspace': MORPHL_CASSANDRA_KEYSPACE,
            'table': 'ga_chp_bq_features_training'}

        (final_df
         .write
         .format('org.apache.spark.sql.cassandra')
         .mode('append')
         .options(**save_options_ga_chp_bq_features_training)
         .save())

        with open(CHURN_THRESHOLD_FILE, 'w') as fh:
            fh.write(str(churn_threshold))
    else:
        final_df = (
            grouped_by_client_id_df
            .select('client_id',
                    'bounces', 'events', 'page_views',
                    'session_duration', 'sessions',
                    'is_desktop', 'is_mobile', 'is_tablet')
            .repartition(32))

        # The schema for final_df is:
        # |-- client_id: string (nullable = true)
        # |-- bounces: double (nullable = true)
        # |-- events: double (nullable = true)
        # |-- page_views: double (nullable = true)
        # |-- session_duration: double (nullable = true)
        # |-- sessions: double (nullable = true)
        # |-- is_desktop: double (nullable = true)
        # |-- is_mobile: double (nullable = true)
        # |-- is_tablet: double (nullable = true)

        final_df.cache()

        final_df.write.parquet(HDFS_DIR_PREDICTION)

        save_options_ga_chp_bq_features_prediction = {
            'keyspace': MORPHL_CASSANDRA_KEYSPACE,
            'table': 'ga_chp_bq_features_prediction'}

        (final_df
         .write
         .format('org.apache.spark.sql.cassandra')
         .mode('append')
         .options(**save_options_ga_chp_bq_features_prediction)
         .save())


if __name__ == '__main__':
    main()
