# Connecting MorphL to BigQuery

<a name="orchestrator-setup"></a>
## Using Model on the MorphL Orchestrator (Prerequisites)

The following steps are required for allowing access to BigQuery:

#### 1. Service Account

A new Google Cloud service account must be created with the following permissions: `BigQuery User` and `Storage Object Creator`.

On the MorphL VM, the service account (JSON format) must be copied to `/opt/secrets/keyfile.json`.

#### 2. BigQuery Source Dataset

The name of the BigQuery dataset that contains the Google Analytics data must exist in `/opt/secrets/src_bq_dataset.txt`.

```
cat > /opt/secrets/src_bq_dataset.txt << EOF
1111111
EOF
```

#### 3. BigQuery Destination Dataset

A BigQuery dataset with the name `bq_avro_morphl` must be created. This dataset will be used as a placeholder for running queries and creating temporary tables before the data is exported to the Google Cloud Storage (GCS).

#### 4. Google Cloud Storage Bucket

A new GCS bucket called `bq_avro_morphl` must be created. This bucket will contain temporary `.avro` files that will be downloaded on the MorphL VM.

#### 5. (Optional) Website URL

If your Google Analytics 360 account contains data from multiple domain names, the website URL must be configured:

```
cat > /opt/secrets/website_url.txt << EOF
www.websitename.com
EOF
```

## Anatomy of the BigQuery extractor

The following commands are part of `runextractor.sh` and their purpose is to create the authenticated environment necessary for the CLI utilities `bq` and `gsutil` to run successfully:

#### 1. Set Google Cloud project and load service account credentials

```
gcloud config set project ${GCP_PROJECT_ID}
gcloud auth activate-service-account --key-file=${KEY_FILE_LOCATION}
bq ls &>/dev/null
```

Note: `bq` and `gsutil` are companion utilities to `gcloud`. All three are installed as components of the Google Cloud SDK.

#### 2. Run query and save results to a temporary BigQuery table

Next, there is a `sed` command that dynamically generates the BQ query to execute by substituting the necessary variables in the template `training/query.sql.template` (for training) or `prediction/query.sql.template` (for predictions).

The query generated above is executed by the BigQuery engine, and the results are saved in the table `DEST_TABLE`:

```
bq query --use_legacy_sql=false --destination_table=${DEST_TABLE} < /opt/code/ingestion/bq_extractor/query.sql &>/dev/null
```

#### 3. Export BigQuery table to `.avro` format

The results of the query are converted into the Avro format and saved to Google Cloud Storage (S3 equivalent in GCP):

```
bq extract --destination_format=AVRO ${DEST_TABLE} ${DEST_GCS_AVRO_FILE}
```

The BigQuery table `DEST_TABLE` is deleted (following a safeguard conditional):

```
echo ${DEST_TABLE} | grep ^bq_avro_morphl.ga_sessions_ && bq rm -f ${DEST_TABLE}
```

#### 4. Download `.avro` file

The resulting Avro file `DEST_GCS_AVRO_FILE` is downloaded from GCS to the local directory `/opt/landing`:

```
gsutil cp ${DEST_GCS_AVRO_FILE} /opt/landing/
```

The remote Avro file `DEST_GCS_AVRO_FILE` is deleted (following a safeguard conditional):

```
echo ${DEST_GCS_AVRO_FILE} | grep '^gs://bq_avro_morphl/ga_sessions_.*.avro$' && gsutil rm ${DEST_GCS_AVRO_FILE}
```

#### 5. Save data to Cassandra

The PySpark script `ga_chp_bq_ingest_avro_file.py` then converts the contents of LOCAL_AVRO_FILE into a DataFrame.  
Finally, the DataFrame is saved to Cassandra.
