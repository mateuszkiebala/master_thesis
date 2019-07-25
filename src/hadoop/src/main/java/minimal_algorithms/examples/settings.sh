#!/usr/bin/env bash

export nn=ip-172-31-15-69.eu-west-2.compute.internal
export port=8020
export src=/home/hadoop/master_thesis/src/hadoop/
export user_path=/user/hadoop/
export items_no=10000000
export partitions_no=6
export reducers_no=6
#export port=9000
#export nn=hadoop1
#export src=/home/mati/magisterka/magisterka/src/hadoop/
#export user_path=/user/mati/
export yarn_libjars=$src/lib/avro-mapred-1.8.1.jar,$src/lib/avro-1.8.1.jar,$src/lib/guava-15.0.jar,$src/lib/avro-tools-1.8.1.jar,$src/lib/snappy-java-1.1.4.jar
export HADOOP_USER_CLASSPATH_FIRST="true"
export HADOOP_CLASSPATH=$src/lib/avro-mapred-1.8.1.jar:$src/lib/avro-1.8.1.jar:$src/lib/guava-15.0.jar:$src/lib/avro-tools-1.8.1.jar:$src/lib/snappy-java-1.1.4.jar
export YARN_HEAPSIZE=1024
export app_jar=$src/target/hadoop-1.0.0-SNAPSHOT.jar
