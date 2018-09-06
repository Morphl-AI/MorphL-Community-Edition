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
HDFS_DIR_TRAINING = f'hdfs://{MORPHL_SERVER_IP_ADDRESS}:{HDFS_PORT}/{DAY_AS_STR}_{UNIQUE_HASH}_preproc_training'
HDFS_DIR_PREDICTION = f'hdfs://{MORPHL_SERVER_IP_ADDRESS}:{HDFS_PORT}/{DAY_AS_STR}_{UNIQUE_HASH}_preproc_prediction'

CHURN_THRESHOLD_FILE = f'{MODELS_DIR}/{DAY_AS_STR}_{UNIQUE_HASH}_churn_threshold.txt'

primary_key = {}

primary_key['ga_cu_df'] = ['client_id','day_of_data_capture']
primary_key['ga_cus_df'] = ['client_id','day_of_data_capture','session_id']

field_baselines = {}

field_baselines['ga_cu_df'] = [
    {'field_name': 'device_category',
     'original_name': 'ga:deviceCategory',
     'needs_conversion': False},
    {'field_name': 'sessions',
     'original_name': 'ga:sessions',
     'needs_conversion': True},
    {'field_name': 'session_duration',
     'original_name': 'ga:sessionDuration',
     'needs_conversion': True},
    {'field_name': 'entrances',
     'original_name': 'ga:entrances',
     'needs_conversion': True},
    {'field_name': 'bounces',
     'original_name': 'ga:bounces',
     'needs_conversion': True},
    {'field_name': 'exits',
     'original_name': 'ga:exits',
     'needs_conversion': True},
    {'field_name': 'page_value',
     'original_name': 'ga:pageValue',
     'needs_conversion': True},
    {'field_name': 'page_load_time',
     'original_name': 'ga:pageLoadTime',
     'needs_conversion': True},
    {'field_name': 'page_load_sample',
     'original_name': 'ga:pageLoadSample',
     'needs_conversion': True}
]

field_baselines['ga_cus_df'] = [
    {'field_name': 'session_count',
     'original_name': 'ga:sessionCount',
     'needs_conversion': True},
    {'field_name': 'days_since_last_session',
     'original_name': 'ga:daysSinceLastSession',
     'needs_conversion': True},
    {'field_name': 'sessions',
     'original_name': 'ga:sessions',
     'needs_conversion': True},
    {'field_name': 'pageviews',
     'original_name': 'ga:pageviews',
     'needs_conversion': True},
    {'field_name': 'unique_pageviews',
     'original_name': 'ga:uniquePageviews',
     'needs_conversion': True},
    {'field_name': 'screen_views',
     'original_name': 'ga:screenViews',
     'needs_conversion': True},
    {'field_name': 'hits',
     'original_name': 'ga:hits',
     'needs_conversion': True},
    {'field_name': 'time_on_page',
     'original_name': 'ga:timeOnPage',
     'needs_conversion': True}
]

def fetch_from_cassandra(c_table_name, spark_session):
    load_options = {
        'keyspace': MORPHL_CASSANDRA_KEYSPACE,
        'table': c_table_name,
        'spark.cassandra.input.fetch.size_in_rows': '150' }

    df = (spark_session.read.format('org.apache.spark.sql.cassandra')
                            .options(**load_options)
                            .load())

    return df

def get_json_schemas(df, spark_session):
    return {
        'json_meta_schema': spark_session.read.json(
            df.limit(10).rdd.map(lambda row: row.json_meta)).schema,
        'json_data_schema': spark_session.read.json(
            df.limit(10).rdd.map(lambda row: row.json_data)).schema}

def zip_lists_full_args(json_meta_dimensions,
                        json_meta_metrics,
                        json_data_dimensions,
                        json_data_metrics,
                        field_attributes,
                        schema_as_list):
    orig_meta_fields = json_meta_dimensions + json_meta_metrics
    orig_meta_fields_set = set(orig_meta_fields)
    for fname in schema_as_list:
        assert(field_attributes[fname]['original_name'] in orig_meta_fields_set), \
            'The field {} is not part of the input record'
    data_values = json_data_dimensions + json_data_metrics[0].values
    zip_list_as_dict = dict(zip(orig_meta_fields,data_values))
    values = [
        zip_list_as_dict[field_attributes[fname]['original_name']]
            for fname in schema_as_list]

    return values

def process(df, primary_key, field_baselines):
    schema_as_list = [
        fb['field_name']
            for fb in field_baselines]

    field_attributes = dict([
        (fb['field_name'],fb)
            for fb in field_baselines])

    meta_fields = [
        'raw_{}'.format(fname) if field_attributes[fname]['needs_conversion'] else fname
            for fname in schema_as_list]

    schema_before_concat = [
        '{}: string'.format(mf) for mf in meta_fields]

    schema = ', '.join(schema_before_concat)

    def zip_lists(json_meta_dimensions,
                  json_meta_metrics,
                  json_data_dimensions,
                  json_data_metrics):
        return zip_lists_full_args(json_meta_dimensions,
                                   json_meta_metrics,
                                   json_data_dimensions,
                                   json_data_metrics,
                                   field_attributes,
                                   schema_as_list)

    zip_lists_udf = f.udf(zip_lists, schema)

    after_zip_lists_udf_df = (
        df.withColumn('all_values', zip_lists_udf('jmeta_dimensions',
                                                  'jmeta_metrics',
                                                  'jdata_dimensions',
                                                  'jdata_metrics')))

    interim_fields_to_select = primary_key + ['all_values.*']

    interim_df = after_zip_lists_udf_df.select(*interim_fields_to_select)

    to_float_udf = f.udf(lambda s: float(s), 'float')

    for fname in schema_as_list:
        if field_attributes[fname]['needs_conversion']:
            fname_raw = 'raw_{}'.format(fname)
            interim_df = interim_df.withColumn(fname, to_float_udf(fname_raw))

    fields_to_select = primary_key + schema_as_list

    result_df = interim_df.select(*fields_to_select)

    return {'result_df': result_df,
            'schema_as_list': schema_as_list}

def prefix_sessions(fname, c):
    return '{}_sessions'.format(c) if fname == 'sessions' else fname

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

    ga_config_df = (
        fetch_from_cassandra('config_parameters', spark_session)
            .filter("morphl_component_name = 'ga_pyspark' AND parameter_name = 'days_worth_of_data_to_load'"))

    days_worth_of_data_to_load = int(ga_config_df.first().parameter_value)

    start_date = ((
        datetime.datetime.now() -
        datetime.timedelta(days=days_worth_of_data_to_load))
            .strftime('%Y-%m-%d'))

    ga_chu_users_df = fetch_from_cassandra('ga_chu_users', spark_session)

    ga_chu_sessions_df = fetch_from_cassandra('ga_chu_sessions', spark_session)

    ga_cu_df = (
        ga_chu_users_df
            .filter("day_of_data_capture >= '{}'".format(start_date)))

    ga_cus_df = (
        ga_chu_sessions_df
            .filter("day_of_data_capture >= '{}'".format(start_date)))

    json_schemas = {}

    json_schemas['ga_cu_df'] = get_json_schemas(ga_cu_df, spark_session)
    json_schemas['ga_cus_df'] = get_json_schemas(ga_cus_df, spark_session)

    after_json_parsing_df = {}

    after_json_parsing_df['ga_cu_df'] = (
        ga_cu_df
            .withColumn('jmeta', f.from_json(
                f.col('json_meta'), json_schemas['ga_cu_df']['json_meta_schema']))
            .withColumn('jdata', f.from_json(
                f.col('json_data'), json_schemas['ga_cu_df']['json_data_schema']))
            .select(f.col('client_id'),
                    f.col('day_of_data_capture'),
                    f.col('jmeta.dimensions').alias('jmeta_dimensions'),
                    f.col('jmeta.metrics').alias('jmeta_metrics'),
                    f.col('jdata.dimensions').alias('jdata_dimensions'),
                    f.col('jdata.metrics').alias('jdata_metrics')))

    after_json_parsing_df['ga_cus_df'] = (
        ga_cus_df
            .withColumn('jmeta', f.from_json(
                f.col('json_meta'), json_schemas['ga_cus_df']['json_meta_schema']))
            .withColumn('jdata', f.from_json(
                f.col('json_data'), json_schemas['ga_cus_df']['json_data_schema']))
            .select(f.col('client_id'),
                    f.col('day_of_data_capture'),
                    f.col('session_id'),
                    f.col('jmeta.dimensions').alias('jmeta_dimensions'),
                    f.col('jmeta.metrics').alias('jmeta_metrics'),
                    f.col('jdata.dimensions').alias('jdata_dimensions'),
                    f.col('jdata.metrics').alias('jdata_metrics')))

    processed_users_dict = process(after_json_parsing_df['ga_cu_df'],
                                   primary_key['ga_cu_df'],
                                   field_baselines['ga_cu_df'])
    users_df = (
        processed_users_dict['result_df']
            .withColumnRenamed('client_id', 'u_client_id')
            .withColumnRenamed('day_of_data_capture', 'u_day_of_data_capture')
            .withColumnRenamed('sessions', 'u_sessions'))

    processed_sessions_dict = process(after_json_parsing_df['ga_cus_df'],
                                      primary_key['ga_cus_df'],
                                      field_baselines['ga_cus_df'])
    sessions_df = (
        processed_sessions_dict['result_df']
            .withColumnRenamed('client_id', 's_client_id')
            .withColumnRenamed('day_of_data_capture', 's_day_of_data_capture')
            .withColumnRenamed('sessions', 's_sessions'))

    joined_df = sessions_df.join(
        users_df, (sessions_df.s_client_id == users_df.u_client_id) &
                  (sessions_df.s_day_of_data_capture == users_df.u_day_of_data_capture))

    s_schema_as_list = [
        prefix_sessions(fname, 's') for fname in processed_sessions_dict['schema_as_list']]

    u_schema_as_list = [
        prefix_sessions(fname, 'u') for fname in processed_users_dict['schema_as_list']]

    tr_raw_fields_to_select = primary_key['ga_cus_df'] + s_schema_as_list + u_schema_as_list

    features_raw_df = (
        joined_df
            .withColumnRenamed('s_client_id', 'client_id')
            .withColumnRenamed('s_day_of_data_capture', 'day_of_data_capture')
            .select(*tr_raw_fields_to_select)
            .withColumn(
                'is_desktop', f.when(
                    f.col('device_category') == 'desktop', 1.0).otherwise(0.0))
            .withColumn(
                'is_mobile', f.when(
                    f.col('device_category') == 'mobile', 1.0).otherwise(0.0))
            .withColumn(
                'is_tablet', f.when(
                    f.col('device_category') == 'tablet', 1.0).otherwise(0.0))
            .drop('device_category')
            .repartition(32))

    features_raw_df.cache()

    features_raw_df.createOrReplaceTempView('features_raw')

    save_options_ga_chu_features_raw = {
        'keyspace': MORPHL_CASSANDRA_KEYSPACE,
        'table': ('ga_chu_features_raw_t' if TRAINING_OR_PREDICTION == 'training' else 'ga_chu_features_raw_p')}

    (features_raw_df
         .write
         .format('org.apache.spark.sql.cassandra')
         .mode('append')
         .options(**save_options_ga_chu_features_raw)
         .save())

    higher_session_counts_sql = 'SELECT * FROM features_raw WHERE session_count > 1'
    higher_session_counts_df = spark_session.sql(higher_session_counts_sql)
    higher_session_counts_df.createOrReplaceTempView('higher_session_counts')

    grouped_by_client_id_sql_parts = [
        'SELECT',
        'client_id,',
        'SUM(pageviews) OVER (PARTITION BY client_id) AS pageviews,'
        'SUM(unique_pageviews) OVER (PARTITION BY client_id) AS unique_pageviews,'
        'SUM(time_on_page) OVER (PARTITION BY client_id) AS time_on_page,'
        'SUM(u_sessions) OVER (PARTITION BY client_id) AS u_sessions,'
        'SUM(session_duration) OVER (PARTITION BY client_id) AS session_duration,'
        'SUM(entrances) OVER (PARTITION BY client_id) AS entrances,'
        'SUM(bounces) OVER (PARTITION BY client_id) AS bounces,'
        'SUM(exits) OVER (PARTITION BY client_id) AS exits,'
        'FIRST_VALUE(is_desktop) OVER (PARTITION BY client_id ORDER BY day_of_data_capture DESC) AS is_desktop,'
        'FIRST_VALUE(is_mobile) OVER (PARTITION BY client_id ORDER BY day_of_data_capture DESC) AS is_mobile,'
        'FIRST_VALUE(is_tablet) OVER (PARTITION BY client_id ORDER BY day_of_data_capture DESC) AS is_tablet,'
        'FIRST_VALUE(session_count) OVER (PARTITION BY client_id ORDER BY day_of_data_capture DESC) AS session_count,'
        'FIRST_VALUE(days_since_last_session) OVER (PARTITION BY client_id ORDER BY day_of_data_capture DESC) AS days_since_last_session,',
        'AVG(days_since_last_session) OVER (PARTITION BY client_id) AS avgdays',
        'FROM',
        'higher_session_counts'
    ]
    grouped_by_client_id_sql = ' '.join(grouped_by_client_id_sql_parts)
    grouped_by_client_id_df = spark_session.sql(grouped_by_client_id_sql)
    grouped_by_client_id_df.createOrReplaceTempView('grouped_by_client_id')

    if TRAINING_OR_PREDICTION == 'training':
        mean_value_of_avg_days_sql = 'SELECT AVG(avgdays) mean_value_of_avgdays FROM grouped_by_client_id'
        mean_value_of_avg_days_df = spark_session.sql(mean_value_of_avg_days_sql)
        churn_threshold = mean_value_of_avg_days_df.first().mean_value_of_avgdays

        final_df = (
            grouped_by_client_id_df
                .withColumn('churned', f.when(
                    f.col('days_since_last_session') > churn_threshold, 1.0).otherwise(0.0))
                .select('client_id',
                        'pageviews', 'unique_pageviews', 'time_on_page',
                        'u_sessions', 'session_duration',
                        'entrances', 'bounces', 'exits', 'session_count',
                        'is_desktop', 'is_mobile', 'is_tablet',
                        'churned')
                .repartition(32))

        final_df.cache()

        final_df.write.parquet(HDFS_DIR_TRAINING)

        save_options_ga_chu_features_training = {
            'keyspace': MORPHL_CASSANDRA_KEYSPACE,
            'table': 'ga_chu_features_training'}

        (final_df
             .write
             .format('org.apache.spark.sql.cassandra')
             .mode('append')
             .options(**save_options_ga_chu_features_training)
             .save())

        with open(CHURN_THRESHOLD_FILE, 'w') as fh:
            fh.write(str(churn_threshold))
    else:
        with open(CHURN_THRESHOLD_FILE, 'r') as fh:
            churn_threshold = fh.read().strip()

        under_threshold_sql = f'SELECT * FROM grouped_by_client_id WHERE avgdays < {churn_threshold}'
        under_threshold_df = spark_session.sql(under_threshold_sql)
        under_threshold_df.createOrReplaceTempView('under_threshold')

        final_df = (
            under_threshold_df
                .select('client_id',
                        'pageviews', 'unique_pageviews', 'time_on_page',
                        'u_sessions', 'session_duration',
                        'entrances', 'bounces', 'exits', 'session_count',
                        'is_desktop', 'is_mobile', 'is_tablet')
                .repartition(32))

        final_df.cache()

        final_df.write.parquet(HDFS_DIR_PREDICTION)

        save_options_ga_chu_features_prediction = {
            'keyspace': MORPHL_CASSANDRA_KEYSPACE,
            'table': 'ga_chu_features_prediction'}

        (final_df
             .write
             .format('org.apache.spark.sql.cassandra')
             .mode('append')
             .options(**save_options_ga_chu_features_prediction)
             .save())

if __name__ == '__main__':
    main()
