#!/usr/bin/env bash
OKGREEN='\033[0;32m'
WARNING='\033[0;31m'
ORANGE='\033[0;35m'
CYAN='\033[1;36m'
COLOR_OFF='\033[0m'

echo "===== Creating group_by user on HDFS ====="
hdfs dfs -mkdir -p /user/group_by

USER_PATH="/user/group_by"
INPUT_TEST="input"
INPUT_HDFS="$USER_PATH/input"

echo "===== Creating input directory on HDFS ====="
hdfs dfs -rm -r $INPUT_HDFS
hdfs dfs -mkdir $INPUT_HDFS

echo "===== Copying test input directory to HDFS ====="
hdfs dfs -put $INPUT_TEST/* $INPUT_HDFS

HDFS="hdfs://192.168.0.199:9000"
NUM_OF_PARTITIONS=10
OUTPUT_HDFS=$USER_PATH

declare -a arr=("sum" "min" "max" "avg")
for i in "${arr[@]}"
do
    hdfs dfs -rm -r "$OUTPUT_HDFS/output_$i"
done

spark-submit --class minimal_algorithms.hadoop.examples.group_by.ExampleGroupBy --master yarn ../../../../target/scala-2.11/library_2.11-0.1.jar "$NUM_OF_PARTITIONS" "$HDFS/$INPUT_HDFS" "$HDFS/$OUTPUT_HDFS"

run() {
    ALGORITHM=$1
    LOGS="result_$ALGORITHM"
    CORRECT_OUT_DIR="output_$ALGORITHM"

    mkdir -p tmp
    rm -rf "tmp/$CORRECT_OUT_DIR"
    hdfs dfs -get "$OUTPUT_HDFS/$CORRECT_OUT_DIR" tmp

    rm -rf $LOGS
    PASSED=0
    ALL=0
    for file in tmp/$CORRECT_OUT_DIR/part-*
    do
        formated_file="$file-formated"
        sed 's/\([[:digit:]]\)\,\([[:digit:]]\)/\1.\2/g' $file > $formated_file
        correct_output="$CORRECT_OUT_DIR/output_$ALL.txt"
        printf "$file <-> $correct_output" >> $LOGS
        if diff -Bb -c $formated_file $correct_output >/dev/null ; then
            PASSED=$((PASSED+1))
            printf "${OKGREEN} OK ${COLOR_OFF}\n" >> $LOGS
        else
            printf "${WARNING} FAIL ${COLOR_OFF}\n" >> $LOGS
        fi
        ALL=$((ALL+1))
    done
    printf "RESULT: ${CYAN}$PASSED${COLOR_OFF} / ${ORANGE}$ALL${COLOR_OFF}\n" >> $LOGS
}

declare -a arr=("sum" "min" "max" "avg")
for i in "${arr[@]}"
do
   run $i
done
