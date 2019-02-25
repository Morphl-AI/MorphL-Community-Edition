MORPHL_PUBLIC_IP_ADDRESS=$(dig +short myip.opendns.com @resolver1.opendns.com)

cp /opt/anaconda/lib/python3.6/site-packages/notebook/notebookapp.py /opt/anaconda/lib/python3.6/site-packages/notebook/notebookapp.py.orig

sed "s/^\(.*socket.gethostname..*\).*$/\1; ip = '${MORPHL_PUBLIC_IP_ADDRESS}'/" /opt/anaconda/lib/python3.6/site-packages/notebook/notebookapp.py.orig > /opt/anaconda/lib/python3.6/site-packages/notebook/notebookapp.py

echo $(hostname) > /opt/spark/conf/slaves

export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook --no-browser --ip=0.0.0.0 --port=8282'

pyspark --jars /opt/spark/jars/spark-cassandra-connector.jar,/opt/spark/jars/jsr166e.jar,/opt/spark/jars/spark-avro.jar --driver-memory 4g
