#!/usr/bin/env bash

MYDIR="${0%/*}"
source "$MYDIR"/../settings.sh

hdfs dfs -rm -r $user_path/prefix_app_output
yarn jar $app_jar minimal_algorithms.hadoop.examples.PrefixApp -libjars $yarn_libjars hdfs://$nn:$port$user_path/ hdfs://$nn:$port$user_path/input.avro hdfs://$nn:$port$user_path/prefix_app_output $items_no $partitions_no $reducers_no
