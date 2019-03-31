export nn=localhost
export data=/home/jsroka/Dropbox/2018_pdd/pdd4_examples_avro_sort
export src=/home/jsroka/Dropbox/2018_pdd/pdd4_examples_avro_sort
export yarn_libjars=$src/lib/avro-mapred-1.8.1.jar,$src/lib/avro-1.8.1.jar,$src/lib/guava-19.0.jar,$src/lib/avro-tools-1.8.1.jar,$src/lib/snappy-java-1.1.4.jar
export HADOOP_USER_CLASSPATH_FIRST="true"
export HADOOP_CLASSPATH=$src/lib/avro-mapred-1.8.1.jar:$src/lib/avro-1.8.1.jar:$src/lib/guava-19.0.jar:$src/lib/snappy-java-1.1.4.jar
export YARN_HEAPSIZE=25000

#upload input into hdfs
hdfs dfs -mkdir -p /user/jsroka
hdfs dfs -rm -r /user/jsroka/input_sample.txt
hdfs dfs -D dfs.replication=3 -put $data/input_sample.txt /user/jsroka/input_sample.txt
hdfs dfs -ls /user/jsroka/
#hdfs dfs -stat /user/jsroka/input_sample.txt
#hdfs fsck /user/jsroka/input_sample.txt
#hdfs fsck -blocks /user/jsroka/input_sample.txt
hdfs dfs -setrep -w 1 /user/jsroka/input_sample.txt
hdfs balancer

hdfs dfs -rm -r /user/jsroka/input.dir
yarn jar $src/dist/SortAvroRecord_przyklad_avro.jar sortavro.record.InputPreprocessing4TextToAvro -libjars $yarn_libjars hdfs://$nn:9000/user/jsroka/input_sample.txt hdfs://$nn:9000/user/jsroka/input.dir
hdfs dfs -setrep -w 1 /user/jsroka/input.dir
hdfs fsck /user/jsroka/input.dir

hdfs dfs -rm -r /user/jsroka/1_sampling_output /user/jsroka/2_sorting_output
yarn jar $src/dist/SortAvroRecord_przyklad_avro.jar sortavro.SortAvroRecord -libjars $yarn_libjars hdfs://$nn:9000/user/jsroka/input.dir hdfs://$nn:9000/user/jsroka/ 100 10 3
#yarn jar $src/dist/SortAvroRecord_przyklad_avro.jar sortavro.SortAvroRecord -libjars $yarn_libjars hdfs://$nn:9000/user/jsroka/input.dir hdfs://$nn:9000/user/jsroka/ 100 10 3 1 >out1.txt 2>&1

#stop-yarn.sh
#stop-dfs.sh
#yarn application -list
#yarn application -kill application_1515534764622_0015

