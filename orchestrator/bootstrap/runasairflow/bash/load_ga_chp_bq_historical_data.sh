# TEMPFILE_A is the duration of the training interval in days
export TEMPFILE_A=$(mktemp)
# TEMPFILE_B is the duration of the predictions interval in days
export TEMPFILE_B=$(mktemp)
# TEMPFILE_C is the Python start date (today)
export TEMPFILE_C=$(mktemp)

python /opt/ga_chp_bq/bq_extractor/ga_chp_bq_load_historical_data.py ${TEMPFILE_A} ${TEMPFILE_B} ${TEMPFILE_C}
rc=$?
if [ ${rc} -eq 0 ]; then
  echo 'Emptying the relevant Cassandra tables ...'
  echo
  cqlsh ${MORPHL_SERVER_IP_ADDRESS} -u morphl -p ${MORPHL_CASSANDRA_PASSWORD} -f /opt/ga_chp_bq/bq_extractor/ga_chp_bq_truncate_tables_before_loading_historical_data.cql

  # Write configuration parameters in corresponding Cassandra table
  DAYS_TRAINING_INTERVAL=$(<${TEMPFILE_A})
  DAYS_PREDICTION_INTERVAL=$(<${TEMPFILE_B})
  sed "s/DAYS_TRAINING_INTERVAL/${DAYS_TRAINING_INTERVAL}/g;s/DAYS_PREDICTION_INTERVAL/${DAYS_PREDICTION_INTERVAL}/g" /opt/ga_chp_bq/training/pipeline_setup/insert_into_ga_chp_bq_config_parameters.cql.template > /tmp/insert_into_config_parameters.cql
  cqlsh ${MORPHL_SERVER_IP_ADDRESS} -u morphl -p ${MORPHL_CASSANDRA_PASSWORD} -f /tmp/insert_into_config_parameters.cql

  # Reset Airflow and create dags
  echo 'Initiating the data load ...'
  echo
  stop_airflow.sh
  rm -rf /home/airflow/airflow/dags/*
  airflow resetdb -y &>/dev/null
  python /opt/orchestrator/bootstrap/runasairflow/python/set_up_airflow_authentication.py
  
  # Create training dag and trigger pipeline
  START_DATE_AS_PY_CODE=$(<${TEMPFILE_C})
  sed "s/START_DATE_AS_PY_CODE/${START_DATE_AS_PY_CODE}/g;s/DAYS_TRAINING_INTERVAL/${DAYS_TRAINING_INTERVAL}/g;s/DAYS_PREDICTION_INTERVAL/${DAYS_PREDICTION_INTERVAL}/g" /opt/ga_chp_bq/training/pipeline_setup/ga_chp_bq_training_airflow_dag.py.template > /home/airflow/airflow/dags/ga_chp_bq_training_pipeline.py
  airflow trigger_dag ga_chp_bq_training_pipeline
  
  # Create prediction dag
  START_DATE_AS_PY_CODE=$(<${TEMPFILE_C})
  sed "s/START_DATE_AS_PY_CODE/${START_DATE_AS_PY_CODE}/g;s/DAYS_PREDICTION_INTERVAL/${DAYS_PREDICTION_INTERVAL}/g" /opt/ga_chp_bq/prediction/pipeline_setup/ga_chp_bq_prediction_airflow_dag.py.template > /home/airflow/airflow/dags/ga_chp_bq_prediction_pipeline.py
  
  start_airflow.sh
  echo 'The data load has been initiated.'
  echo
fi
