cp -r /opt/samplecode /opt/code
cd /opt/code
git pull
spark-submit --jars /opt/spark/jars/spark-cassandra-connector.jar,/opt/spark/jars/jsr166e.jar /opt/code/python/pyspark/ga_pyspark_preprocessor_churned.py
