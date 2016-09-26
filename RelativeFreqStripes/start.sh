#!/bin/bash

JAR_FILE="target/RelativeFreqStripes-1.0-SNAPSHOT.jar"
CLASS_NAME="edu.mum.bigdata.mo.RelativeFreqStripesDriver"
HADOOP_OUTPUT_FOLDER="/user/hive/warehouse/chd_stripes_`date +'%Y%m%d%H%M%S'`"


INPUT_FILES="cust*.txt"
HADOOP_INPUT_DEST="/user/hive/warehouse/custHistData"


mvn clean package
sudo -u hdfs hadoop fs -copyFromLocal $INPUT_FILES $HADOOP_INPUT_DEST 
sudo hadoop jar $JAR_FILE $CLASS_NAME $HADOOP_INPUT_DEST $HADOOP_OUTPUT_FOLDER 

echo "Output written to $HADOOP_OUTPUT_FOLDER"
echo "Output was: "
hadoop fs -cat $HADOOP_OUTPUT_FOLDER/part-r-00000
