stop_hdfs.sh
rm -rf /opt/hadoop/hadoop_store/hdfs/namenode/*
rm -rf /opt/hadoop/hadoop_store/hdfs/datanode/*
hdfs namenode -format &>/dev/null
start_hdfs.sh
