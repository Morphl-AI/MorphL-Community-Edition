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
HDFS_DIR_INPUT = f'hdfs://{MORPHL_SERVER_IP_ADDRESS}:{HDFS_PORT}/{DAY_AS_STR}_{UNIQUE_HASH}_scaled_features_prediction'

class Cassandra:
    def __init__(self):
        self.MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')
        self.MORPHL_CASSANDRA_USERNAME = getenv('MORPHL_CASSANDRA_USERNAME')
        self.MORPHL_CASSANDRA_PASSWORD = getenv('MORPHL_CASSANDRA_PASSWORD')
        self.MORPHL_CASSANDRA_KEYSPACE = getenv('MORPHL_CASSANDRA_KEYSPACE')

        self.CQL_STMT = 'INSERT INTO ga_chu_predictions (client_id,prediction) VALUES (?,?)'

        self.CASS_REQ_TIMEOUT = 3600.0

        self.auth_provider = PlainTextAuthProvider(
            username=self.MORPHL_CASSANDRA_USERNAME,
            password=self.MORPHL_CASSANDRA_PASSWORD)
        self.cluster = Cluster(
            [self.MORPHL_SERVER_IP_ADDRESS], auth_provider=self.auth_provider)
        self.session = self.cluster.connect(self.MORPHL_CASSANDRA_KEYSPACE)

        self.prep_stmt = self.session.prepare(self.CQL_STMT)

    def save_prediction(self, client_id, prediction):
        bind_list = [client_id, prediction]
        self.session.execute(self.prep_stmt, bind_list, timeout=self.CASS_REQ_TIMEOUT)

def batch_inference_on_partition(partition_df):
    churn_model_file = f'/opt/models/{DAY_AS_STR}_{UNIQUE_HASH}_churn_model.h5'
    churn_model = load_model(churn_model_file)
    prediction = churn_model.predict(partition_df.drop(['client_id'], axis=1))[0][0]
    return prediction

def persist_partition(partition_df):
    def persist_one_prediction(series_obj):
        cassandra.save_prediction(series_obj.client_id, series_obj.prediction)
    cassandra = Cassandra()
    partition_df.apply(persist_one_prediction, axis=1)
    return 0

if __name__ == '__main__':
    client = Client()
    dask_df = client.persist(dd.read_parquet(HDFS_DIR_INPUT))
    dask_df.client_id.count().compute()
    dask_df['prediction'] = dask_df.map_partitions(batch_inference_on_partition, meta=('prediction', float))
    dask_df['token'] = dask_df.map_partitions(persist_partition, meta=('token', int))
    dask_df.token.compute()
