#!/usr/bin/env bash

MYDIR="${0%/*}"
source "$MYDIR"/../settings.sh

hdfs dfs -rm -r /user/hadoop/ranking_output
yarn jar $app_jar minimal_algorithms.hadoop.examples.Ranking -libjars $yarn_libjars hdfs://$nn:$port/user/hadoop/ hdfs://$nn:$port/user/hadoop/data_hadoop.avro hdfs://$nn:$port/user/hadoop/ranking_output $items_no $partitions_no $reducers_no
