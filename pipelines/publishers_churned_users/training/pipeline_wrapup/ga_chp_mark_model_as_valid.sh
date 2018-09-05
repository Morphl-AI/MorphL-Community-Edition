IS_MODEL_VALID=True
sed "s/DAY_AS_STR/${DAY_AS_STR}/;s/UNIQUE_HASH/${UNIQUE_HASH}/;s/IS_MODEL_VALID/${IS_MODEL_VALID}/" /opt/ga_chp/training/pipeline_wrapup/insert_into_ga_chp_valid_models.cql.template > /tmp/ga_chp_training_pipeline_insert_into_valid_models.cql
cqlsh ${MORPHL_SERVER_IP_ADDRESS} -u morphl -p ${MORPHL_CASSANDRA_PASSWORD} \
  -f /tmp/ga_chp_training_pipeline_insert_into_valid_models.cql

