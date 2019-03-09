#!/usr/bin/env bash
OKGREEN='\033[0;32m'
WARNING='\033[0;31m'
ORANGE='\033[0;35m'
CYAN='\033[1;36m'
COLOR_OFF='\033[0m'

INPUT_TEST=input
OUTPUT_TEST=window_length_5

#echo "===== Creating test user on HDFS ====="
#hdfs dfs -mkdir -p /user/test

USER_PATH="/user/test"
INPUT_HDFS="$USER_PATH/input"
OUTPUT_HDFS="$USER_PATH/output"

echo "===== Creating input directory on HDFS ====="
hdfs dfs -rm -r $INPUT_HDFS
hdfs dfs -mkdir $INPUT_HDFS

echo "===== Copying test input directory to HDFS ====="
hdfs dfs -put $INPUT_TEST/* $INPUT_HDFS

echo "===== Executing Sliding Aggregation App ... ====="
HDFS="hdfs://192.168.0.220:9000"
hdfs dfs -rm -r $OUTPUT_HDFS
spark-submit --class minimal_algorithms.sliding_aggregation.ExampleSlidingAggregation --master yarn ../../../../target/scala-2.11/library_2.11-0.1.jar "$HDFS/$INPUT_HDFS" 5 "$HDFS/$OUTPUT_HDFS"

mkdir -p tmp
hdfs dfs -get $OUTPUT_HDFS tmp

PASSED=0
ALL=0
for file in tmp/output/part-*
do
    ALL=$((ALL+1))
    correct_output="$OUTPUT_TEST/output_$ALL.txt"
    printf "$file <-> $correct_output"
    if diff -sq -Bb -c $file $correct_output >/dev/null ; then
        PASSED=$((PASSED+1))
        printf "${OKGREEN} OK ${COLOR_OFF}\n"
    else
        printf "${WARNING} FAIL ${COLOR_OFF}\n"
    fi
done
printf "RESULT: ${CYAN}$PASSED${COLOR_OFF} / ${ORANGE}$ALL${COLOR_OFF}\n"
