cqlsh ${MORPHL_SERVER_IP_ADDRESS} -u morphl -p ${MORPHL_CASSANDRA_PASSWORD} \
  -f /opt/ga_chp/prediction/pipeline_setup/ga_chp_truncate_tables_before_prediction_pipeline.cql

HDFS_PORT=9000

HDFS_DIR_PREPROC=hdfs://${MORPHL_SERVER_IP_ADDRESS}:${HDFS_PORT}/${DAY_AS_STR}_${UNIQUE_HASH}_ga_chp_preproc_prediction

HDFS_DIR_SC_FEAT=hdfs://${MORPHL_SERVER_IP_ADDRESS}:${HDFS_PORT}/${DAY_AS_STR}_${UNIQUE_HASH}_ga_chp_scaled_features_prediction

hdfs dfs -rm ${HDFS_DIR_PREPROC}/_metadata/*
hdfs dfs -rmdir ${HDFS_DIR_PREPROC}/_metadata
hdfs dfs -rm ${HDFS_DIR_PREPROC}/*
hdfs dfs -rmdir ${HDFS_DIR_PREPROC}

hdfs dfs -rm ${HDFS_DIR_SC_FEAT}/_metadata/*
hdfs dfs -rmdir ${HDFS_DIR_SC_FEAT}/_metadata
hdfs dfs -rm ${HDFS_DIR_SC_FEAT}/*
hdfs dfs -rmdir ${HDFS_DIR_SC_FEAT}

exit 0
