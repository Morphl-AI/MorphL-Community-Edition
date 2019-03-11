set -e

cp -r /opt/ga_chp_bq /opt/code
cd /opt/code
git pull

# Calculate dates interval depending on the training / prediction setting
if [ "${TRAINING_OR_PREDICTION}" = "training" ]
then
    DATE_TO=$(date --date="${DAY_OF_DATA_CAPTURE} -${PREDICTION_INTERVAL} day -1 day" +%Y-%m-%d)
    DATE_FROM=$(date --date="${DATE_TO} -${TRAINING_INTERVAL} day + 1 day" +%Y-%m-%d)
else
    DATE_TO=$(date --date="${DAY_OF_DATA_CAPTURE} -1 day" +%Y-%m-%d)
    DATE_FROM=$(date --date="${DATE_TO} -${PREDICTION_INTERVAL} day + 1 day" +%Y-%m-%d)
fi

# Get project id from the service account file
GCP_PROJECT_ID=$(jq -r '.project_id' ${KEY_FILE_LOCATION})

# Compose source BQ table name
GA_SESSIONS_DATA_ID=ga_sessions_$(echo ${DATE_FROM} | sed 's/-//g')_$(echo ${DATE_TO} | sed 's/-//g')

# Compose destination BQ table name
DEST_TABLE=${DEST_BQ_DATASET}.${GA_SESSIONS_DATA_ID}

# Compose avro path file for Google Cloud Storage
DEST_GCS_AVRO_FILE=gs://${DEST_GCS_BUCKET}/${GA_SESSIONS_DATA_ID}.avro

# Compose avro path file for local filesystem
WEBSITE_URL=$(</opt/secrets/website_url.txt)
SRC_BQ_DATASET=$(</opt/secrets/src_bq_dataset.txt)
LOCAL_AVRO_FILE=/opt/landing/${DATE_FROM}_${DATE_TO}_${WEBSITE_URL}.avro

# Load Google Cloud service account credentials
gcloud config set project ${GCP_PROJECT_ID}
gcloud auth activate-service-account --key-file=${KEY_FILE_LOCATION}
bq ls &>/dev/null

# Write dynamic variables to the query template file
sed "s/GCP_PROJECT_ID/${GCP_PROJECT_ID}/g;s/SRC_BQ_DATASET/${SRC_BQ_DATASET}/g;s/DATE_FROM/${DATE_FROM}/g;s/DATE_TO/${DATE_TO}/g;s/WEBSITE_URL/${WEBSITE_URL}/g" "/opt/code/${TRAINING_OR_PREDICTION}/query.sql.template" > "/opt/code/${TRAINING_OR_PREDICTION}/query.sql"

# Run query and save result to a temporary BQ destination table 
bq query --use_legacy_sql=false --destination_table=${DEST_TABLE} < "/opt/code/${TRAINING_OR_PREDICTION}/query.sql" &>/dev/null

# Extract destination table to an Avro file from Google Cloud Storage
bq extract --destination_format=AVRO ${DEST_TABLE} ${DEST_GCS_AVRO_FILE}

# Remove temporary destination table
echo ${DEST_TABLE} | grep ^bq_avro_morphl.ga_sessions_ && bq rm -f ${DEST_TABLE}

# Download Avro file from Google Cloud Storage to filesystem
gsutil cp ${DEST_GCS_AVRO_FILE} /opt/landing/

# Remove Avro file from Google Cloud Storage
echo ${DEST_GCS_AVRO_FILE} | grep '^gs://bq_avro_morphl/ga_sessions_.*.avro$' && gsutil rm ${DEST_GCS_AVRO_FILE}

# Copy downloaded Avro file to the landing location
mv /opt/landing/${GA_SESSIONS_DATA_ID}.avro ${LOCAL_AVRO_FILE}
export LOCAL_AVRO_FILE
export WEBSITE_URL

spark-submit --jars /opt/spark/jars/spark-cassandra-connector.jar,/opt/spark/jars/jsr166e.jar,/opt/spark/jars/spark-avro.jar /opt/code/bq_extractor/ga_chp_bq_ingest_avro_file.py
rm ${LOCAL_AVRO_FILE}
