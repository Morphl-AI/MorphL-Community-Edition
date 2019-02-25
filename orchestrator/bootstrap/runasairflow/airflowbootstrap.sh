set -e

unset SUDO_UID SUDO_GID SUDO_USER

ssh-keygen -f ~/.ssh/id_rsa -q -P ''
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

mkdir /home/airflow/.kube
cat /etc/kubernetes/admin.conf > /home/airflow/.kube/config

SP_CASS_CONN_VERSION=2.3.1
JSR166E_VERSION=1.1.0
SPARK_AVRO_VERSION=2.4.0

echo 'Setting up Anaconda ...'
# ANACONDA_SH_URL=$(lynx -dump https://repo.continuum.io/archive/ | grep -o http.*Anaconda3.*Linux.x86_64.sh$ | head -1)
ANACONDA_SH_URL=https://repo.continuum.io/archive/Anaconda3-5.2.0-Linux-x86_64.sh
echo "From ${ANACONDA_SH_URL}"
wget -qO /opt/dockerbuilddirs/pythoncontainer/Anaconda.sh ${ANACONDA_SH_URL}
bash /opt/dockerbuilddirs/pythoncontainer/Anaconda.sh -b -p /opt/anaconda
mv /opt/anaconda/bin/sqlite3 /opt/anaconda/bin/sqlite3.orig
pip install msgpack
pip install --upgrade pip
pip install psycopg2-binary Flask-Bcrypt cassandra-driver graphviz
pip install apache-airflow==1.9.0
pip install scikit-learn==0.20.2
conda install libhdfs3=2.3=3 hdfs3 fastparquet h5py==2.8.0 -y -c conda-forge
conda install python-snappy -y

echo 'Setting up the JDK ...'
JDK_TGZ_URL=$(lynx -dump https://www.azul.com/downloads/zulu/zulu-linux/ | grep -o http.*jdk8.*x64.*gz$ | head -1)
echo "From ${JDK_TGZ_URL}"
wget -qO /opt/tmp/zzzjdk.tgz ${JDK_TGZ_URL}
tar -xf /opt/tmp/zzzjdk.tgz -C /opt
mv /opt/zulu* /opt/jdk
rm /opt/tmp/zzzjdk.tgz

CLOSER="https://www.apache.org/dyn/closer.cgi?as_json=1"
MIRROR=$(curl --stderr /dev/null ${CLOSER} | jq -r '.preferred')

echo 'Setting up Cassandra ...'
CASSANDRA_DIR_URL=$(lynx -dump ${MIRROR}cassandra/ | grep -o 'http.*/cassandra/[0-9].*$' | sort -V | tail -1)
CASSANDRA_TGZ_URL=$(lynx -dump ${CASSANDRA_DIR_URL} | grep -o http.*bin.tar.gz$ | head -1)
echo "From ${CASSANDRA_TGZ_URL}"
wget -qO /opt/tmp/cassandra.tgz ${CASSANDRA_TGZ_URL}
tar -xf /opt/tmp/cassandra.tgz -C /opt
mv /opt/apache-cassandra-* /opt/cassandra
rm /opt/tmp/cassandra.tgz
cp /opt/orchestrator/bootstrap/runasairflow/bash/cassandra/*_cassandra.sh /opt/cassandra/bin/
echo "sed 's/MORPHL_SERVER_IP_ADDRESS/${MORPHL_SERVER_IP_ADDRESS}/g' /opt/orchestrator/bootstrap/runasairflow/templates/cassandra.yaml.template" | bash > /opt/cassandra/conf/cassandra.yaml
start_cassandra.sh

echo 'Setting up Spark ...'
SPARK_DIR_URL=$(lynx -dump ${MIRROR}spark/ | grep -o 'http.*/spark/spark-[0-9].*$' | sort -V | tail -1)
SPARK_TGZ_URL=$(lynx -dump ${SPARK_DIR_URL} | grep -o http.*bin-hadoop.*tgz$ | tail -1)
echo "From ${SPARK_TGZ_URL}"
wget -qO /opt/tmp/zzzspark.tgz ${SPARK_TGZ_URL}
tar -xf /opt/tmp/zzzspark.tgz -C /opt
mv /opt/spark-* /opt/spark
rm /opt/tmp/zzzspark.tgz
cd /opt/spark/conf
sed 's/INFO/FATAL/;s/WARN/FATAL/;s/ERROR/FATAL/' log4j.properties.template > log4j.properties

wget -qO /opt/spark/jars/spark-cassandra-connector.jar https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.11/${SP_CASS_CONN_VERSION}/spark-cassandra-connector_2.11-${SP_CASS_CONN_VERSION}.jar
wget -qO /opt/spark/jars/jsr166e.jar https://repo1.maven.org/maven2/com/twitter/jsr166e/${JSR166E_VERSION}/jsr166e-${JSR166E_VERSION}.jar
wget -qO /opt/spark/jars/spark-avro.jar https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.11/${SPARK_AVRO_VERSION}/spark-avro_2.11-${SPARK_AVRO_VERSION}.jar

echo 'Setting up Hadoop ...'
HADOOP_TGZ_URL=$(lynx -dump ${MIRROR}hadoop/common/stable/ | grep -o http.*gz$ | grep -v src | grep -v site | head -1)
echo "From ${HADOOP_TGZ_URL}"
wget -qO /opt/tmp/zzzhadoop.tgz ${HADOOP_TGZ_URL}
tar -xf /opt/tmp/zzzhadoop.tgz -C /opt
mv /opt/hadoop-* /opt/hadoop
rm /opt/hadoop/bin/*.cmd /opt/hadoop/sbin/*.cmd
rm /opt/tmp/zzzhadoop.tgz
cp /opt/orchestrator/bootstrap/runasairflow/bash/hdfs/*_hdfs.sh /opt/hadoop/bin/
echo "export JAVA_HOME=${JAVA_HOME}" >> /opt/hadoop/etc/hadoop/hadoop-env.sh
echo 'export HADOOP_SSH_OPTS="-o StrictHostKeyChecking=no"' >> /opt/hadoop/etc/hadoop/hadoop-env.sh
mkdir -p /opt/hadoop/hadoop_store/hdfs/namenode
mkdir -p /opt/hadoop/hadoop_store/hdfs/datanode
sed "s/MORPHL_SERVER_IP_ADDRESS/${MORPHL_SERVER_IP_ADDRESS}/g" /opt/orchestrator/bootstrap/runasairflow/templates/core-site.xml.template > /opt/hadoop/etc/hadoop/core-site.xml
cat /opt/orchestrator/bootstrap/runasairflow/templates/hdfs-site.xml.template > /opt/hadoop/etc/hadoop/hdfs-site.xml
echo ${MORPHL_SERVER_FQDN} > /opt/hadoop/etc/hadoop/slaves
/opt/hadoop/bin/hdfs namenode -format &>/dev/null
start_hdfs.sh

cqlsh ${MORPHL_SERVER_IP_ADDRESS} -u cassandra -p cassandra -e "CREATE USER morphl WITH PASSWORD '${MORPHL_CASSANDRA_PASSWORD}' SUPERUSER;"
cqlsh ${MORPHL_SERVER_IP_ADDRESS} -u cassandra -p cassandra -e "ALTER USER cassandra WITH PASSWORD '${NONDEFAULT_SUPERUSER_CASSANDRA_PASSWORD}';"
cqlsh ${MORPHL_SERVER_IP_ADDRESS} -u morphl -p ${MORPHL_CASSANDRA_PASSWORD} -f /opt/ga_chp/cassandra_schema/ga_chp_cassandra_schema.cql
cqlsh ${MORPHL_SERVER_IP_ADDRESS} -u morphl -p ${MORPHL_CASSANDRA_PASSWORD} -f /opt/ga_chp_bq/cassandra_schema/ga_chp_bq_cassandra_schema.cql

mkdir -p /home/airflow/airflow/dags
cat /opt/orchestrator/bootstrap/runasairflow/templates/airflow.cfg.template > /home/airflow/airflow/airflow.cfg
cp /opt/anaconda/bin/airflow /opt/anaconda/bin/airflow_scheduler
cp /opt/anaconda/bin/airflow /opt/anaconda/bin/airflow_webserver
cp /opt/orchestrator/bootstrap/runasairflow/bash/airflow/*_airflow.sh /opt/anaconda/bin/
airflow version
airflow initdb
python /opt/orchestrator/bootstrap/runasairflow/python/set_up_airflow_authentication.py
start_airflow.sh

cd /opt/orchestrator && sudo git pull

cp /opt/orchestrator/dockerbuilddirs/pythoncontainer/Dockerfile /opt/dockerbuilddirs/pythoncontainer/Dockerfile
cp /opt/orchestrator/dockerbuilddirs/pythoncontainer/install.sh /opt/dockerbuilddirs/pythoncontainer/install.sh
cd /opt/dockerbuilddirs/pythoncontainer
docker build -t pythoncontainer .

cp /opt/orchestrator/dockerbuilddirs/pysparkcontainer/Dockerfile /opt/dockerbuilddirs/pysparkcontainer/Dockerfile
cp /opt/orchestrator/dockerbuilddirs/pysparkcontainer/install.sh /opt/dockerbuilddirs/pysparkcontainer/install.sh
cd /opt/dockerbuilddirs/pysparkcontainer
docker build -t pysparkcontainer .

# Spin off temporary container for generating SSL certificates
echo "Generate SSL certificates for API..."
echo ${API_DOMAIN}
cp /opt/orchestrator/dockerbuilddirs/letsencryptcontainer/Dockerfile /opt/dockerbuilddirs/letsencryptcontainer/Dockerfile
sed "s/API_DOMAIN/${API_DOMAIN}/g" /opt/orchestrator/dockerbuilddirs/letsencryptcontainer/default.conf.template > /opt/dockerbuilddirs/letsencryptcontainer/default.conf
echo "Temporary endpoint for generating API SSL certificates with letsencrypt" > /opt/dockerbuilddirs/letsencryptcontainer/site/index.html
cd /opt/dockerbuilddirs/letsencryptcontainer
docker build -t letsencryptnginx .

# Run temporary endpoint on port 80, so it can be reached by Let's Encrypt
docker run -d --name letsencryptcontainer                                              \
           -p 80:80                                                                \
           -v /opt/dockerbuilddirs/letsencryptcontainer/site:/usr/share/nginx/html \
           letsencryptnginx

# Generate SSL certificates.
# Use --staging flag when testing, as Let's Encrypt has a rate limit.
docker run -it --rm                                                                 \
           -v /opt/dockerbuilddirs/letsencryptvolume/etc/letsencrypt:/etc/letsencrypt          \
           -v /opt/dockerbuilddirs/letsencryptvolume/var/lib/letsencrypt:/var/lib/letsencrypt  \
           -v /opt/dockerbuilddirs/letsencryptcontainer/site:/data/letsencrypt                 \
           -v '/opt/dockerbuilddirs/letsencryptvolume/var/log/letsencrypt:/var/log/letsencrypt' \
           certbot/certbot \
           certonly --webroot \
           --register-unsafely-without-email --agree-tos \
           --webroot-path=/data/letsencrypt \
           -d ${API_DOMAIN}

# Stop and remove temporary API endpoint
docker stop letsencryptcontainer && docker rm $_

env | egrep '^MORPHL_SERVER_IP_ADDRESS|^MORPHL_CASSANDRA_USERNAME|^MORPHL_CASSANDRA_PASSWORD|^MORPHL_CASSANDRA_KEYSPACE|^API_DOMAIN|^MORPHL_API_KEY|^MORPHL_API_SECRET|^MORPHL_API_JWT_SECRET|^MORPHL_DASHBOARD_USERNAME|^MORPHL_DASHBOARD_PASSWORD' > /home/airflow/.env_file.sh
kubectl create configmap environment-configmap --from-env-file=/home/airflow/.env_file.sh

# Init auth service
kubectl apply -f /opt/auth/auth_kubernetes_deployment.yaml
kubectl apply -f /opt/auth/auth_kubernetes_service.yaml
AUTH_KUBERNETES_CLUSTER_IP_ADDRESS=$(kubectl get service/auth-service -o jsonpath='{.spec.clusterIP}')
echo "export AUTH_KUBERNETES_CLUSTER_IP_ADDRESS=${AUTH_KUBERNETES_CLUSTER_IP_ADDRESS}" >> /home/airflow/.morphl_environment.sh

# Init GA_CHP service
kubectl apply -f /opt/ga_chp/prediction/model_serving/ga_chp_kubernetes_deployment.yaml
kubectl apply -f /opt/ga_chp/prediction/model_serving/ga_chp_kubernetes_service.yaml
GA_CHP_KUBERNETES_CLUSTER_IP_ADDRESS=$(kubectl get service/ga-chp-service -o jsonpath='{.spec.clusterIP}')
echo "export GA_CHP_KUBERNETES_CLUSTER_IP_ADDRESS=${GA_CHP_KUBERNETES_CLUSTER_IP_ADDRESS}" >> /home/airflow/.morphl_environment.sh

# Init GA_CHP_BQ service
kubectl apply -f /opt/ga_chp_bq/prediction/model_serving/ga_chp_bq_kubernetes_deployment.yaml
kubectl apply -f /opt/ga_chp_bq/prediction/model_serving/ga_chp_bq_kubernetes_service.yaml
GA_CHP_BQ_KUBERNETES_CLUSTER_IP_ADDRESS=$(kubectl get service/ga-chp-bq-service -o jsonpath='{.spec.clusterIP}')
echo "export GA_CHP_BQ_KUBERNETES_CLUSTER_IP_ADDRESS=${GA_CHP_BQ_KUBERNETES_CLUSTER_IP_ADDRESS}" >> /home/airflow/.morphl_environment.sh

sleep 30

# Spin off nginx / API container
echo 'Setting up public facing API ...'
cp /opt/orchestrator/dockerbuilddirs/apicontainer/Dockerfile /opt/dockerbuilddirs/apicontainer/Dockerfile
cp /opt/orchestrator/dockerbuilddirs/apicontainer/nginx.conf /opt/dockerbuilddirs/apicontainer/nginx.conf
sed "s/API_DOMAIN/${API_DOMAIN}/g" /opt/orchestrator/dockerbuilddirs/apicontainer/api.conf.template > /opt/dockerbuilddirs/apicontainer/api.conf

cd /opt/dockerbuilddirs/apicontainer
docker build \
           --build-arg AUTH_KUBERNETES_CLUSTER_IP_ADDRESS=${AUTH_KUBERNETES_CLUSTER_IP_ADDRESS} \
           --build-arg GA_CHP_KUBERNETES_CLUSTER_IP_ADDRESS=${GA_CHP_KUBERNETES_CLUSTER_IP_ADDRESS} \
           --build-arg GA_CHP_BQ_KUBERNETES_CLUSTER_IP_ADDRESS=${GA_CHP_BQ_KUBERNETES_CLUSTER_IP_ADDRESS} \
           -t apinginx .

docker run -d --name apicontainer   \
           -p 80:80 -p 443:443  \
           -v /opt/dockerbuilddirs/letsencryptvolume/etc/letsencrypt:/etc/letsencrypt \
           apinginx

echo 'Testing Kubernetes prediction endpoints ...'

echo 'Testing API ...'
curl -s http://${AUTH_KUBERNETES_CLUSTER_IP_ADDRESS}
curl -s http://${GA_CHP_KUBERNETES_CLUSTER_IP_ADDRESS}/churning
curl -s http://${GA_CHP_BQ_KUBERNETES_CLUSTER_IP_ADDRESS}/churning-bq