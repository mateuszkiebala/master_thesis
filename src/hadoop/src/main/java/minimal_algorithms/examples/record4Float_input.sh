#!/usr/bin/env bash

#upload input into hdfs
hdfs dfs -mkdir -p /user/mati
hdfs dfs -rm -r /user/mati/input_sample_2.txt
hdfs dfs -D dfs.replication=3 -put $data/input_sample_2.txt /user/mati/input_sample_2.txt
hdfs dfs -ls /user/mati/
#hdfs dfs -stat /user/mati/input_sample_2.txt
#hdfs fsck /user/mati/input_sample_2.txt
#hdfs fsck -blocks /user/mati/input_sample_2.txt
hdfs dfs -setrep -w 1 /user/mati/input_sample_2.txt
hdfs balancer

hdfs dfs -rm -r /user/mati/input.dir
yarn jar $app_jar minimal_algorithms.hadoop.examples.input.Record4FloatInputToAvro -libjars $yarn_libjars hdfs://$nn:9000/user/mati/input_sample_2.txt hdfs://$nn:9000/user/mati/input.dir
hdfs dfs -setrep -w 1 /user/mati/input.dir
hdfs fsck /user/mati/input.dir
hdfs fsck /user/mati/data.avro
