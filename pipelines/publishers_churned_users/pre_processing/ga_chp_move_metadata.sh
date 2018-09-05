HDFS_DIR=/${DAY_AS_STR}_${UNIQUE_HASH}_${1}

hdfs dfs -mv ${HDFS_DIR}/_metadata ${HDFS_DIR}/_md
hdfs dfs -mkdir ${HDFS_DIR}/_metadata
hdfs dfs -mv ${HDFS_DIR}/_md ${HDFS_DIR}/_metadata/_metadata

