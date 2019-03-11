from os import getenv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from distributed import Client
from keras.models import load_model

import dask.dataframe as dd

DAY_AS_STR = getenv('DAY_AS_STR')
UNIQUE_HASH = getenv('UNIQUE_HASH')

MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')

HDFS_PORT = 9000
HDFS_DIR_INPUT = f'hdfs://{MORPHL_SERVER_IP_ADDRESS}:{HDFS_PORT}/{DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_scaled_features_prediction'


class Cassandra:
    def __init__(self):
        self.MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')
        self.MORPHL_CASSANDRA_USERNAME = getenv('MORPHL_CASSANDRA_USERNAME')
        self.MORPHL_CASSANDRA_PASSWORD = getenv('MORPHL_CASSANDRA_PASSWORD')
        self.MORPHL_CASSANDRA_KEYSPACE = getenv('MORPHL_CASSANDRA_KEYSPACE')

        self.prep_stmt = {}

        template_for_prediction = 'INSERT INTO ga_chp_predictions (client_id,prediction) VALUES (?,?)'
        template_for_predictions_by_date = 'INSERT INTO ga_chp_predictions_by_prediction_date (prediction_date, client_id, prediction) VALUES (?,?,?)'
        template_for_predictions_statistics = 'UPDATE ga_chp_predictions_statistics SET loyal=loyal+?, neutral=neutral+?, churning=churning+?, lost=lost+? WHERE prediction_date=?'

        self.CASS_REQ_TIMEOUT = 3600.0

        self.auth_provider = PlainTextAuthProvider(
            username=self.MORPHL_CASSANDRA_USERNAME,
            password=self.MORPHL_CASSANDRA_PASSWORD)
        self.cluster = Cluster(
            [self.MORPHL_SERVER_IP_ADDRESS], auth_provider=self.auth_provider)
        self.session = self.cluster.connect(self.MORPHL_CASSANDRA_KEYSPACE)

        self.prep_stmt['prediction'] = self.session.prepare(
            template_for_prediction)
        self.prep_stmt['predictions_by_date'] = self.session.prepare(
            template_for_predictions_by_date)
        self.prep_stmt['predictions_statistics'] = self.session.prepare(
            template_for_predictions_statistics)

    def update_predictions_statistics(self, series_obj):

        loyal = series_obj[series_obj <= 0.4].count().compute()

        neutral = series_obj[(series_obj  > 0.4) & (series_obj <= 0.6)].count().compute()

        churning = series_obj[(series_obj > 0.6) & (series_obj <= 0.9)].count().compute()

        lost = series_obj[(series_obj > 0.9) & (series_obj <= 1)].count().compute()

        bind_list = [loyal, neutral, churning, lost, DAY_AS_STR]

        self.session.execute(
            self.prep_stmt['predictions_statistics'], bind_list, timeout=self.CASS_REQ_TIMEOUT)

    def save_prediction_by_date(self, client_id, prediction):
        bind_list = [DAY_AS_STR, client_id, prediction]

        self.session.execute(
            self.prep_stmt['predictions_by_date'], bind_list, timeout=self.CASS_REQ_TIMEOUT)

    def save_prediction(self, client_id, prediction):
        bind_list = [client_id, prediction]

        self.session.execute(self.prep_stmt['prediction'], bind_list,
                             timeout=self.CASS_REQ_TIMEOUT)


def batch_inference_on_partition(partition_df):
    churn_model_file = f'/opt/models/{DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_churn_model.h5'
    churn_model = load_model(churn_model_file)
    prediction = churn_model.predict(
        partition_df.drop(['client_id'], axis=1))[0][0]
    return prediction


def persist_partition(partition_df):
    def persist_one_prediction(series_obj):
        cassandra.save_prediction_by_date(series_obj.client_id, series_obj.prediction)
        cassandra.save_prediction(series_obj.client_id, series_obj.prediction)
    cassandra = Cassandra()
    partition_df.apply(persist_one_prediction, axis=1)
    return 0


if __name__ == '__main__':
    client = Client()
    cassandra = Cassandra()
    dask_df = client.persist(dd.read_parquet(HDFS_DIR_INPUT))
    dask_df.client_id.count().compute()
    dask_df['prediction'] = dask_df.map_partitions(
        batch_inference_on_partition, meta=('prediction', float))
    cassandra.update_predictions_statistics(dask_df['prediction'])
    dask_df['token'] = dask_df.map_partitions(
        persist_partition, meta=('token', int))
    dask_df.token.compute()
