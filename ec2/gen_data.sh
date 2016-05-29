#!/bin/bash
if [ "$#" -ne 1 ]; then
  echo "Usage: gen_data.sh <num slaves>"
  exit 1
fi

source ~/scripts/common.sh

set -e

# HDFS won't let you overwrite files/directories
if $($HADOOP_INSTALL/bin/hdfs dfs -test -d /data); then
  $HADOOP_INSTALL/bin/hdfs dfs -rm -r /data
fi
if $($HADOOP_INSTALL/bin/hdfs dfs -test -d /output); then
  $HADOOP_INSTALL/bin/hdfs dfs -rm -r /output
fi
$HADOOP_INSTALL/bin/hdfs dfs -mkdir /data
$HADOOP_INSTALL/bin/hdfs dfs -mkdir /output

# Generate data
if [ ! -f /tmp/kmeans.txt ]; then
  $SPARK_HOME/bin/spark-submit --class "KMeansDataGenerator" \
    --master $SPARK_MASTER \
    target/scala-2.10/data-generator_2.10-1.0.jar /tmp/kmeans.txt 10 60000000
fi
$HADOOP_INSTALL/bin/hdfs dfs -copyFromLocal -f /tmp/kmeans.txt $HDFS/data/kmeans_data

if [ ! -f /tmp/lr_data.txt ]; then
  $SPARK_HOME/bin/spark-submit --class "LRDataGenerator" \
    --master $SPARK_MASTER \
    target/scala-2.10/data-generator_2.10-1.0.jar /tmp/lr.txt 10 60000000
fi
$HADOOP_INSTALL/bin/hdfs dfs -copyFromLocal -f /tmp/lr.txt $HDFS/data/lr_data
