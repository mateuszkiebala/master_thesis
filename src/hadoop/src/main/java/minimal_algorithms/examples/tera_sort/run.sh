#!/usr/bin/env bash

MYDIR="${0%/*}"
source "$MYDIR"/../settings.sh

hdfs dfs -rm -r $user_path/terasort_output
yarn jar $app_jar minimal_algorithms.hadoop.examples.TeraSort -libjars hdfs://$nn:$port$user_path/ hdfs://$nn:$port$user_path/data_hadoop.avro hdfs://$nn:$port$user_path/terasort_output 100 2 2
