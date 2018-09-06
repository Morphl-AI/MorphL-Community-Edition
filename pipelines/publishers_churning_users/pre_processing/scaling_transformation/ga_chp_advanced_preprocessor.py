from os import getenv
from distributed import Client
import dask.dataframe as dd
from scaler_transformer import ScalerTransformer

DAY_AS_STR = getenv('DAY_AS_STR')
UNIQUE_HASH = getenv('UNIQUE_HASH')

TRAINING_OR_PREDICTION = getenv('TRAINING_OR_PREDICTION')

MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')

HDFS_PORT = 9000
HDFS_DIR_INPUT_TRAINING = f'hdfs://{MORPHL_SERVER_IP_ADDRESS}:{HDFS_PORT}/{DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_preproc_training'
HDFS_DIR_OUTPUT_TRAINING = f'hdfs://{MORPHL_SERVER_IP_ADDRESS}:{HDFS_PORT}/{DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_scaled_features_training'
HDFS_DIR_INPUT_PREDICTION = f'hdfs://{MORPHL_SERVER_IP_ADDRESS}:{HDFS_PORT}/{DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_preproc_prediction'
HDFS_DIR_OUTPUT_PREDICTION = f'hdfs://{MORPHL_SERVER_IP_ADDRESS}:{HDFS_PORT}/{DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_scaled_features_prediction'

def process_dataframe(client, hdfs_dir_input, hdfs_dir_output):
    dask_df = client.persist(dd.read_parquet(hdfs_dir_input))
    st = ScalerTransformer(dask_df)
    scaled_features = st.get_transformed_data()
    scaled_features.repartition(npartitions=32).to_parquet(hdfs_dir_output)

def main():
    client = Client()
    if TRAINING_OR_PREDICTION == 'training':
        process_dataframe(client, HDFS_DIR_INPUT_TRAINING, HDFS_DIR_OUTPUT_TRAINING)
    else:
        process_dataframe(client, HDFS_DIR_INPUT_PREDICTION, HDFS_DIR_OUTPUT_PREDICTION)

if __name__ == '__main__':
    main()

