#!/usr/bin/env bash

MYDIR="${0%/*}"
source "$MYDIR"/../settings.sh

hdfs dfs -mkdir -p /user/mati
hdfs dfs -rm -r /user/mati/input_semi_join.txt
hdfs dfs -D dfs.replication=3 -put $data/input_semi_join.txt /user/mati/input_semi_join.txt
hdfs dfs -ls /user/mati/
hdfs dfs -setrep -w 1 /user/mati/input_semi_join.txt
hdfs balancer

hdfs dfs -rm -r /user/mati/input_semi_join.dir
yarn jar $app_jar minimal_algorithms.examples.input.SemiJoinInputToAvro -libjars $yarn_libjars hdfs://$nn:9000/user/mati/input_semi_join.txt hdfs://$nn:9000/user/mati/input_semi_join.dir
hdfs dfs -setrep -w 1 /user/mati/input_semi_join.dir
hdfs fsck /user/mati/input_semi_join.dir

hdfs dfs -rm -r /user/mati/tmp
hdfs dfs -rm -r /user/mati/semi_join_output
yarn jar $app_jar minimal_algorithms.examples.SemiJoin -libjars $yarn_libjars hdfs://$nn:9000/user/mati/input_semi_join.dir hdfs://$nn:9000/user/mati/ 20 3 3
