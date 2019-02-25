export DEBIAN_FRONTEND=noninteractive
apt update -qq &>/dev/null
apt -y install locales apt-utils &>/dev/null
echo 'en_US.UTF-8 UTF-8' > /etc/locale.gen
locale-gen > /dev/null
update-locale LANG=en_US.UTF-8
apt -y install wget curl git vim bzip2 jq mc lynx net-tools less tmux sqlite3 sudo ca-certificates build-essential binutils python2.7-minimal &>/dev/null
bash /opt/Anaconda.sh -b -p /opt/anaconda
rm /opt/Anaconda.sh
mv /opt/anaconda/bin/sqlite3 /opt/anaconda/bin/sqlite3.orig
pip install msgpack
pip install --upgrade pip
pip install google-auth google-api-python-client tensorflow keras cassandra-driver PyJWT flask-cors
pip install scikit-learn==0.20.2
conda install libhdfs3=2.3=3 hdfs3 fastparquet h5py==2.8.0 -y -c conda-forge
conda install python-snappy -y
wget -qO /opt/gcsdk.tgz https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz
tar -xf /opt/gcsdk.tgz -C /opt
mv /opt/google-cloud-sdk /opt/gcsdk
/opt/gcsdk/install.sh --quiet --usage-reporting=false &>/dev/null
echo 'Building container 1 (out of 2), this may take a while ...'
