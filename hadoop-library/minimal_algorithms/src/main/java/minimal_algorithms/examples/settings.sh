#!/usr/bin/env bash
export nn=hadoop1
export data=/home/mati/magisterka/hadoop-library/minimal_algorithms/input
export src=/home/mati/magisterka/hadoop-library/minimal_algorithms
export yarn_libjars=$src/lib/avro-mapred-1.8.1.jar,$src/lib/avro-1.8.1.jar,$src/lib/guava-15.0.jar,$src/lib/avro-tools-1.8.1.jar,$src/lib/snappy-java-1.1.4.jar
export HADOOP_USER_CLASSPATH_FIRST="true"
export HADOOP_CLASSPATH=$src/lib/avro-mapred-1.8.1.jar:$src/lib/avro-1.8.1.jar:$src/lib/guava-15.0.jar:$src/lib/avro-tools-1.8.1.jar:$src/lib/snappy-java-1.1.4.jar
export YARN_HEAPSIZE=1024
export app_jar=$src/target/minimal_algorithms-1.0.0.jar

#upload input into hdfs
hdfs dfs -mkdir -p /user/mati
hdfs dfs -rm -r /user/mati/input_sample_2.txt
hdfs dfs -D dfs.replication=3 -put $data/input_sample_2.txt /user/mati/input_sample_2.txt
hdfs dfs -ls /user/mati/
#hdfs dfs -stat /user/mati/input_sample_2.txt
#hdfs fsck /user/mati/input_sample_2.txt
#hdfs fsck -blocks /user/mati/input_sample_2.txt
hdfs dfs -setrep -w 1 /user/mati/input_sample_2.txt
hdfs balancer

hdfs dfs -rm -r /user/mati/input.dir
yarn jar $app_jar minimal_algorithms.examples.types.InputPreprocessing4TextToAvro -libjars $yarn_libjars hdfs://$nn:9000/user/mati/input_sample_2.txt hdfs://$nn:9000/user/mati/input.dir
hdfs dfs -setrep -w 1 /user/mati/input.dir
hdfs fsck /user/mati/input.dir
