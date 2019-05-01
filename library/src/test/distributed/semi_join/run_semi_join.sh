#!/usr/bin/env bash
OKGREEN='\033[0;32m'
WARNING='\033[0;31m'
ORANGE='\033[0;35m'
CYAN='\033[1;36m'
COLOR_OFF='\033[0m'

echo "===== Creating semi_join user on HDFS ====="
hdfs dfs -mkdir -p /user/semi_join

USER_PATH="/user/semi_join"
INPUT_R_TEST="input_R"
INPUT_T_TEST="input_T"
INPUT_R_HDFS="$USER_PATH/$INPUT_R_TEST"
INPUT_T_HDFS="$USER_PATH/$INPUT_T_TEST"

echo "===== Creating input directory on HDFS ====="
hdfs dfs -rm -r $INPUT_R_HDFS
hdfs dfs -rm -r $INPUT_T_HDFS
hdfs dfs -mkdir $INPUT_R_HDFS
hdfs dfs -mkdir $INPUT_T_HDFS

echo "===== Copying test input directories to HDFS ====="
hdfs dfs -put $INPUT_R_TEST/* $INPUT_R_HDFS
hdfs dfs -put $INPUT_T_TEST/* $INPUT_T_HDFS

HDFS="hdfs://192.168.0.220:9000"
OUTPUT_HDFS="$USER_PATH/output"

hdfs dfs -rm -r $OUTPUT_HDFS
spark-submit --class minimal_algorithms.examples.semi_join.ExampleSemiJoin --master yarn ../../../../target/scala-2.11/library_2.11-0.1.jar 10 "$HDFS/$INPUT_R_HDFS" "$HDFS/$INPUT_T_HDFS" "$HDFS/$OUTPUT_HDFS"

run() {
    LOGS="result"
    CORRECT_OUT_DIR="output"

    mkdir -p tmp
    rm -rf "tmp/$CORRECT_OUT_DIR"
    hdfs dfs -get $OUTPUT_HDFS tmp

    rm -rf $LOGS
    PASSED=0
    ALL=0
    for file in tmp/$CORRECT_OUT_DIR/part-*
    do
        correct_output="$CORRECT_OUT_DIR/output_$ALL.txt"
        printf "$file <-> $correct_output" >> $LOGS
        if diff -Bb -c $file $correct_output >/dev/null ; then
            PASSED=$((PASSED+1))
            printf "${OKGREEN} OK ${COLOR_OFF}\n" >> $LOGS
        else
            printf "${WARNING} FAIL ${COLOR_OFF}\n" >> $LOGS
        fi
        ALL=$((ALL+1))
    done
    printf "RESULT: ${CYAN}$PASSED${COLOR_OFF} / ${ORANGE}$ALL${COLOR_OFF}\n" >> $LOGS
}

run
