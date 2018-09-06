cql_stmt='SELECT day_as_str, unique_hash, is_model_valid FROM morphl.ga_chp_valid_models WHERE always_zero = 0 AND is_model_valid = True LIMIT 1 ALLOW FILTERING;'
cqlsh_output=$(cqlsh ${MORPHL_SERVER_IP_ADDRESS} -u morphl -p ${MORPHL_CASSANDRA_PASSWORD} -e "${cql_stmt}" | grep True | sed 's/ //g')
if [ -n ${cqlsh_output} ]; then
  echo ${cqlsh_output} | cut -d'|' -f1 > /tmp/ga_chp_prediction_pipeline_day_as_str.txt
  echo ${cqlsh_output} | cut -d'|' -f2 > /tmp/ga_chp_prediction_pipeline_unique_hash.txt
  exit 0
else
  exit 1
fi

