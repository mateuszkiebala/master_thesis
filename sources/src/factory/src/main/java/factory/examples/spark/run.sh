#!/usr/bin/env bash

OKGREEN='\033[0;32m'
WARNING='\033[0;31m'
ORANGE='\033[0;35m'
CYAN='\033[1;36m'
COLOR_OFF='\033[0m'

echo "===== Creating prefix user on HDFS ====="
hdfs dfs -mkdir -p /user/prefix

USER_PATH="/user/prefix"
INPUT_TEST="input"
INPUT_HDFS="$USER_PATH/input"

echo "===== Creating input directory on HDFS ====="
hdfs dfs -rm -r $INPUT_HDFS
hdfs dfs -mkdir $INPUT_HDFS

echo "===== Copying test input directory to HDFS ====="
hdfs dfs -put $INPUT_TEST/* $INPUT_HDFS

HDFS="hdfs://192.168.0.199:9000"
NUM_OF_PARTITIONS=3
OUTPUT_HDFS="$USER_PATH/output_prefix"

hdfs dfs -rm -r $OUTPUT_HDFS
spark-submit --class minimal_algorithms.factory.examples.spark.ExamplePrefix --master yarn ../../../../../../target/factory-1.0.0-SNAPSHOT-jar-with-dependencies.jar "$NUM_OF_PARTITIONS" "$HDFS/$INPUT_HDFS" "$HDFS/$OUTPUT_HDFS"
