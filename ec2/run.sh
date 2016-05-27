#!/bin/bash

# Script to run on EC2 nodes.

set -e

cd "$(dirname "$0")"

HADOOP_CONF_DIR="$HADOOP_INSTALL/etc/hadoop"
cp mapred-site.xml "$HADOOP_CONF_DIR/mapred-site.xml"
cp core-site.xml "$HADOOP_CONF_DIR/core-site.xml"
cp masters "$HADOOP_CONF_DIR/masters"
cp slaves "$HADOOP_CONF_DIR/slaves"
cp slaves "$SPARK_HOME/conf/slaves"

# Master stuff
if [ -f ~/scripts/is_master ]; then
  set -x
  start-dfs.sh
  start-yarn.sh
  hdfs namenode -format -force
  "$SPARK_HOME/sbin/start-all.sh"

  SPARK_MASTER=spark://`hostname`:7077
  spark-submit \
    --master $SPARK_MASTER \
    --class org.apache.spark.examples.SparkPi \
    --num-executors 10 \
    ~/spark/examples/target/scala-2.10/spark-examples-1.6.1-hadoop2.7.2.jar \
    1000
  # scala KMeansDataGenerator.scala $SPARK_MASTER test 10 3 5
fi
