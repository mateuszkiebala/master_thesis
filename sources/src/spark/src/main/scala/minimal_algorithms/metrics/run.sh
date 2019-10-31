#!/usr/bin/env bash

nn=ip-172-31-46-228.eu-west-2.compute.internal
port=8020
src=/home/hadoop/master_thesis/src/spark/
user_path=/user/hadoop/
items_no=10000000
partitions_no=-1
app_jar=$src/target/spark-1.0.0-SNAPSHOT.jar

#spark-submit --packages org.apache.spark:spark-avro_2.11:2.4.2 --class minimal_algorithms.spark.metrics.TeraSort --master yarn $app_jar $partitions_no hdfs://$nn:$port$user_path/data_hadoop.avro $items_no
spark-submit --packages org.apache.spark:spark-avro_2.11:2.4.2 --class minimal_algorithms.spark.metrics.PrefixTest --deploy-mode cluster --master yarn --num-executors 5 --executor-cores 3 --executor-memory 2g $app_jar $partitions_no hdfs://$nn:$port$user_path/data_hadoop.avro $items_no
