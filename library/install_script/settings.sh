export download_dir=~/hadoop/download
export install_superdir=~/hadoop
export data_superdir=~/hadoop/library

#export hdfs_dir="$( mktemp -d /tmp/hadoop.XXXXXX )"
export hdfs_dir=${data_superdir}
export datanode_dir=${hdfs_dir}/datanode
export namenode_dir=${hdfs_dir}/namenode
export user=mk359758

export JAVA_HOME=$install_superdir/jdk1.8.0_202
export HADOOP_INSTALL=$install_superdir/hadoop-2.8.3
export SPARK_HOME=$install_superdir/spark-2.3.0-bin-hadoop2.7
export HADOOP_PREFIX=$HADOOP_INSTALL 
export PATH=$JAVA_HOME/bin:$HADOOP_INSTALL/bin:$HADOOP_INSTALL/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

export etc_hadoop=${HADOOP_INSTALL}/etc/hadoop


