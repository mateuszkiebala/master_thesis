# Prefix without library
Spark-scala implementation of `prefix sum` algorithm described in http://www.cse.cuhk.edu.hk/~taoyf/paper/sigmod13-mr.pdf

### Run
spark-submit --class PrefixApp --master yarn prefixapp.jar [arguments]

Arguments:
1. hdfs_path_to_input
2. number_of_partitions
3. hdfs_path_to_output
