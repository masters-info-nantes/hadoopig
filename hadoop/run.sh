#!/bin/bash
rm -rf out
mvn clean install
$HADOOP_ROOT/bin/hadoop jar target/hadooping-1.0-SNAPSHOT.jar App ../input/input.txt out