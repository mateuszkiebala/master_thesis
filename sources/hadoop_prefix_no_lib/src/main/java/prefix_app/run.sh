#!/usr/bin/env bash

#export nn=192.168.0.199
#export port=8020
#export src=/home/hadoop/master_thesis/src/hadoop/
#export user_path=/user/hadoop/
export items_no=20
export partitions_no=3
export reducers_no=3
export port=9000
export nn=hadoop1
export src=/home/mati/magisterka/magisterka/hadoop_prefix_no_lib
export user_path=/user/mati/
export HADOOP_USER_CLASSPATH_FIRST="true"
export app_jar=$src/target/minimal_algorithms-hadoop-prefix-no-lib-1.0.0-SNAPSHOT.jar

hdfs dfs -rm -r $user_path/sampling
hdfs dfs -rm -r $user_path/sorting
hdfs dfs -rm -r $user_path/part_stats
hdfs dfs -rm -r $user_path/prefix
yarn jar $app_jar prefix_app.PrefixApp hdfs://$nn:$port$user_path/ hdfs://$nn:$port$user_path/input.in hdfs://$nn:$port$user_path/prefix_output $items_no $partitions_no $reducers_no