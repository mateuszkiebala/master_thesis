#!/usr/bin/env bash

MYDIR="${0%/*}"
source "$MYDIR"/../settings.sh
source "$MYDIR"/../record4Float_input.sh

hdfs dfs -rm -r /user/mati/perfect_sort_with_ranks_output
yarn jar $app_jar minimal_algorithms.hadoop.examples.PerfectSortWithRanks -libjars $yarn_libjars hdfs://$nn:9000/user/mati/ hdfs://$nn:9000/user/mati/input.dir hdfs://$nn:9000/user/mati/perfect_sort_with_ranks_output 20 3 3
