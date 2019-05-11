#!/usr/bin/env bash
export nn=hadoop1
export data=/home/mati/magisterka/hadoop-library/minimal_algorithms
export src=/home/mati/magisterka/hadoop-library/minimal_algorithms
export yarn_libjars=$src/lib/avro-mapred-1.8.1.jar,$src/lib/avro-1.8.1.jar,$src/lib/guava-15.0.jar,$src/lib/avro-tools-1.8.1.jar,$src/lib/snappy-java-1.1.4.jar
export HADOOP_USER_CLASSPATH_FIRST="true"
export HADOOP_CLASSPATH=$src/lib/avro-mapred-1.8.1.jar:$src/lib/avro-1.8.1.jar:$src/lib/guava-15.0.jar:$src/lib/avro-tools-1.8.1.jar:$src/lib/snappy-java-1.1.4.jar
export YARN_HEAPSIZE=1024

#upload input into hdfs
##hdfs dfs -mkdir -p /user/mati
##hdfs dfs -rm -r /user/mati/input_sample_2.txt
##hdfs dfs -D dfs.replication=3 -put $data/input_sample_2.txt /user/mati/input_sample_2.txt
##hdfs dfs -ls /user/mati/
#hdfs dfs -stat /user/mati/input_sample_2.txt
#hdfs fsck /user/mati/input_sample_2.txt
#hdfs fsck -blocks /user/mati/input_sample_2.txt
##hdfs dfs -setrep -w 1 /user/mati/input_sample_2.txt
##hdfs balancer

##hdfs dfs -rm -r /user/mati/input.dir
##yarn jar $src/dist/SortAvroRecord_przyklad_avro.jar minimal_algorithms.record.InputPreprocessing4TextToAvro -libjars $yarn_libjars hdfs://$nn:9000/user/mati/input_sample_2.txt hdfs://$nn:9000/user/mati/input.dir
##hdfs dfs -setrep -w 1 /user/mati/input.dir
##hdfs fsck /user/mati/input.dir

##hdfs dfs -rm -r /user/mati/1_sampling_output
##hdfs dfs -rm -r /user/mati/2_sorting_output
hdfs dfs -rm -r /user/mati/3_ranking_output
hdfs dfs -rm -r /user/mati/4_partition_statistics_output
hdfs dfs -rm -r /user/mati/5_prefix_output
hdfs dfs -rm -r /user/mati/6_group_by_output
yarn jar $src/target/minimal_algorithms-1.0-SNAPSHOT.jar minimal_algorithms.SortAvroRecord -libjars $yarn_libjars hdfs://$nn:9000/user/mati/input.dir hdfs://$nn:9000/user/mati/ 20 3 3
#yarn jar $src/dist/SortAvroRecord_przyklad_avro.jar minimal_algorithms.SortAvroRecord -libjars $yarn_libjars hdfs://$nn:9000/user/mati/input.dir hdfs://$nn:9000/user/mati/ 100 10 3 1 >out1.txt 2>&1

#stop-yarn.sh
#stop-dfs.sh
#yarn application -list
#yarn application -kill application_1515534764622_0015
