#!/usr/bin/env bash
OKGREEN='\033[0;32m'
WARNING='\033[0;31m'
ORANGE='\033[0;35m'
CYAN='\033[1;36m'
COLOR_OFF='\033[0m'

echo "===== Creating sliding_aggregation user on HDFS ====="
hdfs dfs -mkdir -p /user/sliding_aggregation

USER_PATH="/user/sliding_aggregation"
INPUT_TEST="input"
INPUT_HDFS="$USER_PATH/input"

echo "===== Creating input directory on HDFS ====="
hdfs dfs -rm -r $INPUT_HDFS
hdfs dfs -mkdir $INPUT_HDFS

echo "===== Copying test input directory to HDFS ====="
hdfs dfs -put $INPUT_TEST/* $INPUT_HDFS

HDFS="hdfs://192.168.0.199:9000"

run() {
    NUM_OF_PARTITIONS=$1
    WINDOW_LENGTH=$2
    CORRECT_OUT_DIR="window_length_$WINDOW_LENGTH"
    OUTPUT_HDFS="$USER_PATH/$CORRECT_OUT_DIR"
    LOGS="result_$WINDOW_LENGTH"

    hdfs dfs -rm -r $OUTPUT_HDFS
    spark-submit --class minimal_algorithms.spark.examples.sliding_aggregation.ExampleSlidingAggregation --master yarn ../../../../target/spark-1.0.0-SNAPSHOT.jar "$NUM_OF_PARTITIONS" "$WINDOW_LENGTH" "$HDFS/$INPUT_HDFS" "$HDFS/$OUTPUT_HDFS"

    mkdir -p tmp
    rm -rf "tmp/$CORRECT_OUT_DIR"
    hdfs dfs -get $OUTPUT_HDFS tmp

    rm -rf $LOGS
    PASSED=0
    ALL=0
    for file in tmp/$CORRECT_OUT_DIR/part-*
    do
        ALL=$((ALL+1))
        correct_output="$CORRECT_OUT_DIR/output_$ALL.txt"
        printf "$file <-> $correct_output" >> $LOGS
        if diff -Bb -c $file $correct_output >/dev/null ; then
            PASSED=$((PASSED+1))
            printf "${OKGREEN} OK ${COLOR_OFF}\n" >> $LOGS
        else
            printf "${WARNING} FAIL ${COLOR_OFF}\n" >> $LOGS
        fi
    done
    printf "RESULT: ${CYAN}$PASSED${COLOR_OFF} / ${ORANGE}$ALL${COLOR_OFF}\n" >> $LOGS
}

declare -a arr=("5" "7" "17" "3000")
for i in "${arr[@]}"
do
   run 10 $i
done
