#!/usr/bin/env bash

MYDIR="${0%/*}"
source "$MYDIR"/../settings.sh

hdfs dfs -rm -r /user/mati/perfect_sort_output
yarn jar $app_jar minimal_algorithms.examples.PerfectSort -libjars $yarn_libjars hdfs://$nn:9000/user/mati/input.dir hdfs://$nn:9000/user/mati/ 20 3 3
