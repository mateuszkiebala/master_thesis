#!/usr/bin/env bash

MYDIR="${0%/*}"
source "$MYDIR"/../settings.sh
source "$MYDIR"/../record4Float_input.sh

hdfs dfs -rm -r /user/mati/partition_statistics_output
yarn jar $app_jar minimal_algorithms.hadoop.examples.PartitionStatistics -libjars $yarn_libjars hdfs://$nn:9000/user/mati/ hdfs://$nn:9000/user/mati/data.avro hdfs://$nn:9000/user/mati/partition_statistics_output 1000 3 3
