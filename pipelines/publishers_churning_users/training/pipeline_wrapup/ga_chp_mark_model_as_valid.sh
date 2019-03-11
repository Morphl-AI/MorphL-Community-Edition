IS_MODEL_VALID=True

# Read churn threshold from text file
CHURN_THRESHOLD_FILE=${MODELS_DIR}/${DAY_AS_STR}_${UNIQUE_HASH}_ga_chp_churn_threshold.txt
THRESHOLD=$(<$CHURN_THRESHOLD_FILE)

# Read model accuracy and loss from json file
SCORES_FILE=${MODELS_DIR}/${DAY_AS_STR}_${UNIQUE_HASH}_ga_chp_churn_scores.json
ACCURACY=$(cat ${SCORES_FILE} | jq '.accuracy')
LOSS=$(cat ${SCORES_FILE} | jq '.loss')

# Insert model stats into the Cassandra database
sed "s/DAY_AS_STR/${DAY_AS_STR}/;s/UNIQUE_HASH/${UNIQUE_HASH}/;s/ACCURACY/${ACCURACY}/;s/LOSS/${LOSS}/;s/THRESHOLD/${THRESHOLD}/;s/IS_MODEL_VALID/${IS_MODEL_VALID}/" /opt/ga_chp/training/pipeline_wrapup/insert_into_ga_chp_valid_models.cql.template > /tmp/ga_chp_training_pipeline_insert_into_valid_models.cql
cqlsh ${MORPHL_SERVER_IP_ADDRESS} -u morphl -p ${MORPHL_CASSANDRA_PASSWORD} \
  -f /tmp/ga_chp_training_pipeline_insert_into_valid_models.cql

