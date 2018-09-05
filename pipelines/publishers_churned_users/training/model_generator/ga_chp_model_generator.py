from os import getenv
from distributed import Client
import dask.dataframe as dd
from model_generator import ModelGenerator

DAY_AS_STR = getenv('DAY_AS_STR')
UNIQUE_HASH = getenv('UNIQUE_HASH')

TRAINING_OR_PREDICTION = getenv('TRAINING_OR_PREDICTION')

MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')

HDFS_PORT = 9000
HDFS_DIR_INPUT = f'hdfs://{MORPHL_SERVER_IP_ADDRESS}:{HDFS_PORT}/{DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_scaled_features_training'

def main():
    client = Client()
    dask_df = client.persist(dd.read_parquet(HDFS_DIR_INPUT))
    ModelGenerator(dask_df).generate_and_save_model()

if __name__ == '__main__':
    main()

