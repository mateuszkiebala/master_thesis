#!/usr/bin/env bash

set -e
SRC="/home/mati/magisterka/src"

mvn install:install-file -Dfile="$SRC/hadoop/target/hadoop-1.0.0-SNAPSHOT.jar"\
  -DgroupId=minimal_algorithms -DartifactId=hadoop -Dversion=1.0.0-SNAPSHOT -Dpackaging=jar -DgeneratePom=true

mvn install:install-file -Dfile="$SRC/spark/target/spark-1.0.0-SNAPSHOT.jar"\
  -DgroupId=minimal_algorithms -DartifactId=spark -Dversion=1.0.0-SNAPSHOT -Dpackaging=jar -DgeneratePom=true

mvn package
