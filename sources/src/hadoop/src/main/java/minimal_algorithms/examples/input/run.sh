#!/usr/bin/env bash

export items_no=20
export partitions_no=1
export reducers_no=1
export port=9000
export nn=hadoop1
export src=/home/mati/magisterka/magisterka/src/hadoop/
export user_path=/user/mati/
export yarn_libjars=$src/lib/avro-mapred-1.8.1.jar,$src/lib/avro-1.8.1.jar,$src/lib/guava-15.0.jar,$src/lib/avro-tools-1.8.1.jar,$src/lib/snappy-java-1.1.4.jar
export HADOOP_USER_CLASSPATH_FIRST="true"
export HADOOP_CLASSPATH=$src/lib/avro-mapred-1.8.1.jar:$src/lib/avro-1.8.1.jar:$src/lib/guava-15.0.jar:$src/lib/avro-tools-1.8.1.jar:$src/lib/snappy-java-1.1.4.jar
export app_jar=$src/target/hadoop-1.0.0-SNAPSHOT.jar

hdfs dfs -rm -r $user_path/prefix_output
yarn jar $app_jar minimal_algorithms.hadoop.examples.input.FourIntsInputToAvro -libjars $yarn_libjars hdfs://$nn:$port$user_path/input.txt hdfs://$nn:$port$user_path/output