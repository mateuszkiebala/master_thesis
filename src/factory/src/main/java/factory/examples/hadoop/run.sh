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
AVRO_INPUT_HDFS="$USER_PATH/avro_input"

echo "===== Creating input.in directory on HDFS ====="
hdfs dfs -rm -r $INPUT_HDFS
hdfs dfs -rm -r $AVRO_INPUT_HDFS
hdfs dfs -mkdir $INPUT_HDFS

echo "===== Copying test input.in directory to HDFS ====="
hdfs dfs -put $INPUT_TEST/* $INPUT_HDFS

HDFS="hdfs://192.168.0.199:9000"
OUTPUT_HDFS="$USER_PATH/output_prefix"

hdfs dfs -rm -r $OUTPUT_HDFS
app_jar=/home/mati/magisterka/src/factory/target/factory-1.0.0-SNAPSHOT-jar-with-dependencies.jar

yarn jar $app_jar minimal_algorithms.hadoop.examples.input.Record4FloatInputToAvro "$HDFS/$INPUT_HDFS/" "$HDFS/$AVRO_INPUT_HDFS/"
yarn jar $app_jar minimal_algorithms.factory.examples.hadoop.ExamplePrefix "$HDFS/$USER_PATH" "$HDFS/$AVRO_INPUT_HDFS" "$HDFS/$OUTPUT_HDFS" 20 3 3
