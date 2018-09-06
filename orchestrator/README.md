# MorphL Platform Orchestrator

The MorphL Orchestrator is the backbone of the MorphL platform. It sets up the infrastructure and software that are necessary for running the MorphL platform. It consists of 3 pipelines:

- **Ingestion Pipeline** - It runs a series of connectors responsible for gathering data from various APIs (Google Analytics, Mixpanel, etc.) and save it into Cassandra tables.

- **Training Pipeline** - Consists of pre-processors (responsible for cleaning, formatting, deduplicating, normalizing and transforming data) and model training.

- **Prediction Pipeline** - It generates predictions based on the model that was trained. It is triggered at the final step of the ingestion pipeline through a preflight check.

The pipelines are set up using [Apache Airflow](https://github.com/apache/incubator-airflow).

### Prerequisites

The orchestrator can be installed on a virtual instance on a cloud platform of your choice (Google Cloud Platform, Amazon Web Services, etc.).

We recommend using a clean Ubuntu 16.04 machine, minimum 2 vCPUs, 16GB of RAM, 50GB storage.

## Quick Start Guide

### Step 1) Installing the platform

This step is required for setting up the environment and downloading the required software on your instance.

Bootstrap the installation by running the following commands as root:

```
WHERE_THE_ORCHESTRATOR_IS='https://github.com/Morphl-AI/MorphL-Orchestrator'
WHERE_GA_CHP_IS='https://github.com/Morphl-AI/MorphL-Model-Publishers-Churning-Users'

apt update -qq && apt -y install git ca-certificates

git clone ${WHERE_THE_ORCHESTRATOR_IS} /opt/orchestrator
git clone ${WHERE_GA_CHP_IS} /opt/ga_chp

bash /opt/orchestrator/bootstrap/runasroot/rootbootstrap.sh
```

The installation process is fully automated and will take a while to complete (25-35 minutes). The `rootbootstrap.sh` script will install Docker, Docker Registry, Kubernetes, PostgreSQL and various utilities libraries. A second script (`airflowbootstrap.sh`) will be run and will install Anaconda, Airflow, JDK, Cassandra, Spark and Hadoop.

Once the installation is done, check the bottom of the output to see the if the status `The installation has completed successfully.` has been reported.

At this point a few more setup steps are necessary.

### Step 2) Providing connectors credentials

The next step is creating a series of files that store credentials for connecting to various data sources APIs.

From the root prompt, log into `airflow`:

```
su - airflow
```

#### Step 2.1) Google Analytics

Connecting to **Google Analytics API v4** requires creating a service account and retrieving a view ID from your Google Analytics dashboard. The orchestrator assumes that your Google Analytics dashboard has already been configured to allow exporting of granular data (at the browser & session level). You can read [here](https://github.com/Morphl-Project/MorphL-Collectors/tree/master/google-analytics) about the required setup and **creating a service account**.

Paste your key file into `/opt/secrets/keyfile.json` and your view ID into `/opt/secrets/viewid.txt`, possibly using syntax like this:

```
cat > /opt/secrets/keyfile.json << EOF
{
...supersecretkeyfilecontents...
}
EOF

cat > /opt/secrets/viewid.txt << EOF
123123456456123123
EOF
```

Log out of `airflow` and back in again, and verify that your key file and view ID have been configured correctly:

```
cat /opt/secrets/keyfile.json

env | grep VIEW_ID
```

If the output of `env | grep VIEW_ID` is empty, like this:

```
VIEW_ID=
```

it means you have forgotten to log out of `airflow` and back in again.

Unless specified otherwise, all commands referred to below should be run as user `airflow`.

### Step 3) Loading historical data

To train the models, you'll need to bring in historical data. If you don't have historical data, you can let the ingestion pipeline gather it. However, in most cases, you'll have data that was already gathered and can be immediately downloaded.

Run the command:

```
load_ga_chp_historical_data.sh
```

You will be presented with a prompt that lets you select the time interval for loading the data:

```
How much historical data should be loaded?

1) 2018-08-04 - present time (5 days worth of data)
2) 2018-07-30 - present time (10 days worth of data)
3) 2018-07-10 - present time (30 days worth of data)
4) 2018-06-10 - present time (60 days worth of data)
5) 2018-04-11 - present time (120 days worth of data)
6) 2018-02-10 - present time (180 days worth of data)
7) 2017-11-12 - present time (270 days worth of data)
8) 2017-08-09 - present time (365 days worth of data)

Select one of the numerical options 1 thru 8:
```

Once you select an option, you should see an output like this:

```
Emptying the relevant Cassandra tables ...

Initiating the data load ...

The data load has been initiated.
```

Open [http://???.???.???.???:8181/admin/](http://???.???.???.???:8181/admin/) in a browser.  
`???.???.???.???` is the Internet-facing IP address of the Ubuntu machine.  
You should be able to get this IP address from your cloud management interface or by running:

```
dig +short myip.opendns.com @resolver1.opendns.com
```

To visualize the pipelines' status, logs, etc. you can log into Airflow's web UI.

Use username `airflow` and the password found with:

```
env | grep AIRFLOW_WEB_UI_PASSWORD
```

Keep refreshing the UI page until all the data for the number of days you specified previously, has been loaded into Cassandra.

### Step 4) Scheduling the remaining parts of the pipeline

Once all the raw data has been loaded, there is one more thing to do for the ML pipeline to be fully operational:

```
airflow trigger_dag ga_chp_training_pipeline
```

The command above will trigger the training pipeline, and upon running it you should see output similar to this:

```
[...] {__init__.py:45} INFO - Using executor LocalExecutor
[...] {models.py:189} INFO - Filling up the DagBag from /home/airflow/airflow/dags
[...] {cli.py:203} INFO - Created <DagRun ga_chp_training_pipeline, externally triggered: True>
```

Since we have already loaded historical data (step 3), we can start running the pre-processors and train the models. If you do not manually trigger the training pipeline as described above, it will automatically start at its scheduled date (it runs on a weekly basis).

The step above only needs to be performed once, immediately following the installation.

From this point forward, **the platform is on auto-pilot** and will on a regular basis collect new data and generate fresh ML models fully automatically.

### Using Predictions

Once a model has been trained, the prediction pipeline also needs to be triggered. You can wait until it is automatically triggered by the preflight check at the end of the ingestion pipeline (which runs daily) or you can trigger it yourself with the following command:

```
airflow trigger_dag ga_chp_prediction_pipeline
```

After the pipeline is triggered, predictions can be accessed using the following command:

```
curl -s http://${GA_CHP_KUBERNETES_CLUSTER_IP_ADDRESS}/getprediction/GA1
```

`GA1` represents a client ID.

### Troubleshooting

Should you need the connection details for Cassandra, the user name is `morphl` and you can find the password with:

```
env | grep MORPHL_CASSANDRA_PASSWORD
```

[TBA] - debugging pipeline errors (Airflow INFO vs ERROR logs)

### (Optional) PySpark development

Since running PySpark on your local machine can be challenging, we recommend using the MorphL Orchestrator.

To start developing PySpark applications, you need to run the Jupyter Notebook with a very specific configuration.  
To do that, you have at your disposal a script that sets up that environment:

```
run_pyspark_notebook.sh
```

Look for these messages in the output:

```
[I 14:01:20.091 NotebookApp] The Jupyter Notebook is running at:
[I 14:01:20.091 NotebookApp] http://???.???.???.???:8282/?token=2501b8f79e8f128a01e83a457311514e021f0e33c70690cb
```

It is recommended that every PySpark notebook should have this snippet at the top:

```
from os import getenv

MASTER_URL = 'local[*]'
APPLICATION_NAME = 'preprocessor'

MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')
MORPHL_CASSANDRA_USERNAME = getenv('MORPHL_CASSANDRA_USERNAME')
MORPHL_CASSANDRA_PASSWORD = getenv('MORPHL_CASSANDRA_PASSWORD')
MORPHL_CASSANDRA_KEYSPACE = getenv('MORPHL_CASSANDRA_KEYSPACE')

spark.stop()

spark_session = (
    SparkSession.builder
                .appName(APPLICATION_NAME)
                .master(MASTER_URL)
                .config('spark.cassandra.connection.host', MORPHL_SERVER_IP_ADDRESS)
                .config('spark.cassandra.auth.username', MORPHL_CASSANDRA_USERNAME)
                .config('spark.cassandra.auth.password', MORPHL_CASSANDRA_PASSWORD)
                .config('spark.sql.shuffle.partitions', 16)
                .getOrCreate())

log4j = spark_session.sparkContext._jvm.org.apache.log4j
log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
```
