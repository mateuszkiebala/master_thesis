#!/usr/bin/env bash

MYDIR="${0%/*}"
source "$MYDIR"/../settings.sh

hdfs dfs -rm -r /user/spark/prefix_output
yarn jar $app_jar minimal_algorithms.hadoop.examples.Prefix -libjars $yarn_libjars hdfs://$nn:8020/user/spark/ hdfs://$nn:8020/user/spark/data_hadoop.avro hdfs://$nn:8020/user/spark/prefix_output 10000 10 10
