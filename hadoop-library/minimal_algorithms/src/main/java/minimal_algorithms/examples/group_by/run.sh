#!/usr/bin/env bash

MYDIR="${0%/*}"
source "$MYDIR"/../settings.sh

hdfs dfs -rm -r /user/mati/group_by_output
yarn jar $app_jar minimal_algorithms.examples.GroupBy -libjars $yarn_libjars hdfs://$nn:9000/user/mati/input.dir hdfs://$nn:9000/user/mati/ 20 3 3
