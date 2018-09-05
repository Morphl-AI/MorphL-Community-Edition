cp -r /opt/ga_chp /opt/code
cd /opt/code
git pull
spark-submit --jars /opt/spark/jars/spark-cassandra-connector.jar,/opt/spark/jars/jsr166e.jar /opt/code/pre_processing/basic_processing/ga_chp_basic_preprocessor.py

