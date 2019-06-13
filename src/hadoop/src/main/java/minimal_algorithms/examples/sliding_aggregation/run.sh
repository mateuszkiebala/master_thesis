#!/usr/bin/env bash

MYDIR="${0%/*}"
source "$MYDIR"/../settings.sh
#source "$MYDIR"/../record4Float_input.sh

hdfs dfs -rm -r /user/mati/sliding_aggregation_output
yarn jar $app_jar minimal_algorithms.hadoop.examples.SlidingAggregation -libjars $yarn_libjars hdfs://$nn:9000/user/mati/ hdfs://$nn:9000/user/mati/input.dir hdfs://$nn:9000/user/mati/sliding_aggregation_output 20 3 3 7
