set -e

apt -y install docker.io apt-transport-https curl
echo 'DOCKER_OPTS="--insecure-registry localhost:5000"' > /etc/default/docker
service docker restart
docker pull registry:2
docker run -d --name registry --restart=always    \
           -p 127.0.0.1:5000:5000                 \
           -v /var/lib/registry:/var/lib/registry \
           registry:2

# STABLE_KUBERNETES_VERSION=$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)
STABLE_KUBERNETES_VERSION=1.11.3
curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
echo "deb http://apt.kubernetes.io/ kubernetes-xenial main" > /etc/apt/sources.list.d/kubernetes.list
APT_KUBERNETES_VERSION=$(echo ${STABLE_KUBERNETES_VERSION} | sed 's/^v//')-00
apt update -qq && apt -y install kubelet=${APT_KUBERNETES_VERSION} kubeadm=${APT_KUBERNETES_VERSION} kubectl=${APT_KUBERNETES_VERSION}
kubeadm config images pull --kubernetes-version=${STABLE_KUBERNETES_VERSION}
kubeadm init --kubernetes-version=${STABLE_KUBERNETES_VERSION} --pod-network-cidr=10.244.0.0/16
export KUBECONFIG=/etc/kubernetes/admin.conf
echo -e '\nexport KUBECONFIG=/etc/kubernetes/admin.conf' >> /root/.bashrc
chmod g+r /etc/kubernetes/admin.conf
chgrp sudo /etc/kubernetes/admin.conf
kubectl apply -f https://raw.githubusercontent.com/coreos/flannel/master/Documentation/kube-flannel.yml
kubectl taint nodes --all node-role.kubernetes.io/master-

apt -y install build-essential binutils ntp openssl sudo wget lynx htop nethogs tmux jq graphviz python2.7
apt -y install postgresql postgresql-contrib postgresql-client postgresql-client-common
sudo -Hiu postgres psql -c "CREATE USER airflow PASSWORD 'airflow';"
sudo -Hiu postgres psql -c "CREATE DATABASE airflow;"
sudo -Hiu postgres psql -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;"
sudo -Hiu postgres psql -c "CREATE USER morphl PASSWORD 'morphl';"
sudo -Hiu postgres psql -c "CREATE DATABASE morphl;"
sudo -Hiu postgres psql -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO morphl;"

cat /opt/orchestrator/bootstrap/runasroot/rc.local > /etc/rc.local

# Generate passwords and API credentials
new_hex_digest () {
 openssl rand -hex 64 | cut -c1-$1
}

MORPHL_SERVER_IP_ADDRESS=$(ip route get $(ip r | grep ^default | cut -d' ' -f3) | awk '{print $NF; exit}')
MORPHL_SERVER_FQDN=$(hostname -f)
AIRFLOW_OS_PASSWORD=$(new_hex_digest 20)
AIRFLOW_WEB_UI_PASSWORD=$(new_hex_digest 20)
MORPHL_CASSANDRA_PASSWORD=$(new_hex_digest 20)
NONDEFAULT_SUPERUSER_CASSANDRA_PASSWORD=$(new_hex_digest 20)
MORPHL_API_KEY="pk_$(new_hex_digest 20)"
MORPHL_API_SECRET="sk_$(new_hex_digest 20)"
MORPHL_API_JWT_SECRET=$(new_hex_digest 20)
MORPHL_DASHBOARD_USERNAME="morphl_$(new_hex_digest 10)"
MORPHL_DASHBOARD_PASSWORD=$(new_hex_digest 20)

useradd -m airflow
echo "airflow:${AIRFLOW_OS_PASSWORD}" | chpasswd
usermod -aG docker,sudo airflow

touch /home/airflow/.profile /home/airflow/.morphl_environment.sh /home/airflow/.morphl_secrets.sh
chmod 660 /home/airflow/.profile /home/airflow/.morphl_environment.sh /home/airflow/.morphl_secrets.sh
chown airflow /home/airflow/.profile /home/airflow/.morphl_environment.sh /home/airflow/.morphl_secrets.sh
echo "airflow ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers
echo "morphl ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers
echo "export ENVIRONMENT_TYPE=production" >> /home/airflow/.morphl_environment.sh
echo "export MORPHL_SERVER_IP_ADDRESS=${MORPHL_SERVER_IP_ADDRESS}" >> /home/airflow/.morphl_environment.sh
echo "export MORPHL_SERVER_FQDN=${MORPHL_SERVER_FQDN}" >> /home/airflow/.morphl_environment.sh
echo "export AIRFLOW_HOME=/home/airflow/airflow" >> /home/airflow/.morphl_environment.sh
echo "export AIRFLOW_GPL_UNIDECODE=yes" >> /home/airflow/.morphl_environment.sh
echo "export JAVA_HOME=/opt/jdk" >> /home/airflow/.morphl_environment.sh
echo "export SPARK_HOME=/opt/spark" >> /home/airflow/.morphl_environment.sh
echo "export CASSANDRA_HOME=/opt/cassandra" >> /home/airflow/.morphl_environment.sh
echo "export MORPHL_CASSANDRA_USERNAME=morphl" >> /home/airflow/.morphl_environment.sh
echo "export MORPHL_CASSANDRA_KEYSPACE=morphl" >> /home/airflow/.morphl_environment.sh
echo "export LIBHDFS3_CONF=/opt/hadoop/etc/hadoop/hdfs-site.xml" >> /home/airflow/.morphl_environment.sh
echo "export LD_LIBRARY_PATH=/opt/hadoop/lib/native:\$LD_LIBRARY_PATH" >> /home/airflow/.morphl_environment.sh
echo "export API_DOMAIN=$(</opt/settings/apidomain.txt)" >> /home/airflow/.morphl_environment.sh
echo "export PATH=/opt/orchestrator/bootstrap/runasairflow/bash:/opt/anaconda/bin:/opt/jdk/bin:/opt/spark/bin:/opt/cassandra/bin:/opt/hadoop/bin:\$PATH" >> /home/airflow/.morphl_environment.sh
echo "export KEY_FILE_LOCATION=/opt/secrets/keyfile.json" >> /home/airflow/.morphl_secrets.sh
echo "export VIEW_ID=\$(</opt/secrets/viewid.txt)" >> /home/airflow/.morphl_secrets.sh
echo "export AIRFLOW_OS_PASSWORD=${AIRFLOW_OS_PASSWORD}" >> /home/airflow/.morphl_secrets.sh
echo "export AIRFLOW_WEB_UI_PASSWORD=${AIRFLOW_WEB_UI_PASSWORD}" >> /home/airflow/.morphl_secrets.sh
echo "export MORPHL_CASSANDRA_PASSWORD=${MORPHL_CASSANDRA_PASSWORD}" >> /home/airflow/.morphl_secrets.sh
echo "export NONDEFAULT_SUPERUSER_CASSANDRA_PASSWORD=${NONDEFAULT_SUPERUSER_CASSANDRA_PASSWORD}" >> /home/airflow/.morphl_secrets.sh
echo "export MORPHL_API_KEY=${MORPHL_API_KEY}" >> /home/airflow/.morphl_secrets.sh
echo "export MORPHL_API_SECRET=${MORPHL_API_SECRET}" >> /home/airflow/.morphl_secrets.sh
echo "export MORPHL_API_JWT_SECRET=${MORPHL_API_JWT_SECRET}" >> /home/airflow/.morphl_secrets.sh
echo "export MORPHL_DASHBOARD_USERNAME=${MORPHL_DASHBOARD_USERNAME}" >> /home/airflow/.morphl_secrets.sh
echo "export MORPHL_DASHBOARD_PASSWORD=${MORPHL_DASHBOARD_PASSWORD}" >> /home/airflow/.morphl_secrets.sh
echo ". /home/airflow/.morphl_environment.sh" >> /home/airflow/.profile
echo ". /home/airflow/.morphl_secrets.sh" >> /home/airflow/.profile

mkdir -p /opt/dockerbuilddirs/{pythoncontainer,pysparkcontainer,letsencryptcontainer,apicontainer}
mkdir -p /opt/dockerbuilddirs/letsencryptcontainer/site
mkdir /opt/{models,secrets,landing,tmp}
touch /opt/secrets/{keyfile.json,viewid.txt}
chmod 775 /opt /opt/{models,secrets,landing,tmp}
chmod 660 /opt/secrets/{keyfile.json,viewid.txt}
chmod -R 775 /opt/dockerbuilddirs
chgrp airflow /opt /opt/{models,secrets,landing,tmp} /opt/secrets/{keyfile.json,viewid.txt}
chgrp -R airflow /opt/dockerbuilddirs

sudo -Hiu airflow bash -c /opt/orchestrator/bootstrap/runasairflow/airflowbootstrap.sh

echo
echo 'The installation has completed successfully.'
echo