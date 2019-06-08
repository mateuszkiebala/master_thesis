#!/usr/bin/env bash

export nn=hadoop1
export data=/home/mati/magisterka/hadoop-library/minimal_algorithms/src/main/java/minimal_algorithms/examples/input
export src=/home/mati/magisterka/hadoop-library/minimal_algorithms
export yarn_libjars=$src/lib/avro-mapred-1.8.1.jar,$src/lib/avro-1.8.1.jar,$src/lib/guava-15.0.jar,$src/lib/avro-tools-1.8.1.jar,$src/lib/snappy-java-1.1.4.jar
export HADOOP_USER_CLASSPATH_FIRST="true"
export HADOOP_CLASSPATH=$src/lib/avro-mapred-1.8.1.jar:$src/lib/avro-1.8.1.jar:$src/lib/guava-15.0.jar:$src/lib/avro-tools-1.8.1.jar:$src/lib/snappy-java-1.1.4.jar
export YARN_HEAPSIZE=1024
export app_jar=$src/target/minimal_algorithms-1.0.0.jar
