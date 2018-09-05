cql_stmt='SELECT is_model_valid FROM morphl.ga_chp_valid_models WHERE always_zero = 0 AND is_model_valid = True LIMIT 1 ALLOW FILTERING;'
  cqlsh ${MORPHL_SERVER_IP_ADDRESS} -u morphl -p ${MORPHL_CASSANDRA_PASSWORD} -e "${cql_stmt}" | grep True && \
    airflow trigger_dag ga_chp_prediction_pipeline
exit 0

