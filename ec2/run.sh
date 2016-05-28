#!/bin/bash

# Script to run on EC2 nodes.

set -e

cd "$(dirname "$0")"

HDFS="hdfs://`hostname`:54310"
HADOOP_CONF_DIR="$HADOOP_INSTALL/etc/hadoop"
cp mapred-site.xml "$HADOOP_CONF_DIR/mapred-site.xml"
cp core-site.xml "$HADOOP_CONF_DIR/core-site.xml"
cp masters "$HADOOP_CONF_DIR/masters"
cp slaves "$HADOOP_CONF_DIR/slaves"
cp slaves "$SPARK_HOME/conf/slaves"

# Master stuff
if [ -f ~/scripts/is_master ]; then
  set -x
  "$HADOOP_INSTALL/sbin/start-dfs.sh"
  "$HADOOP_INSTALL/sbin/start-yarn.sh"
  "$HADOOP_INSTALL/bin/hdfs" namenode -format -force
  "$SPARK_HOME/sbin/start-all.sh"

  SPARK_MASTER=spark://`hostname`:7077

  # Can't use spark's sbt for some reason
  echo "deb https://dl.bintray.com/sbt/debian /" | \
    sudo tee -a /etc/apt/sources.list.d/sbt.list
  sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823
  sudo apt-get update
  sudo apt-get install sbt
  
  # HDFS won't let you overwrite files/directories
  if $($HADOOP_INSTALL/bin/hdfs dfs -test -d /data)
    then $HADOOP_INSTALL/bin/hdfs dfs -rm -r /data
  fi
  $HADOOP_INSTALL/bin/hdfs dfs -mkdir /data

  sbt package

  # Generate data
  $SPARK_HOME/bin/spark-submit --class "KMeansDataGenerator" \
    --master $SPARK_MASTER \
    target/scala-2.10/data-generator_2.10-1.0.jar \
      $SPARK_MASTER $HDFS/data/kmeans_data 10 3 10
  $SPARK_HOME/bin/spark-submit --class "LRDataGenerator" \
    --master $SPARK_MASTER \
    target/scala-2.10/data-generator_2.10-1.0.jar \
      $SPARK_MASTER $HDFS/data/lr_data 10 3 10

  # Run ML algorithms
  $SPARK_HOME/bin/spark-submit --class org.apache.spark.examples.SparkKMeans \
    --master $SPARK_MASTER \
    $SPARK_HOME/examples/target/scala-2.10/spark-examples-1.6.1-hadoop2.7.2.jar\
      $HDFS/data/kmeans_data 3 0.5
  $SPARK_HOME/bin/spark-submit --class org.apache.spark.examples.SparkHdfsLR \
    --master $SPARK_MASTER \
    $SPARK_HOME/examples/target/scala-2.10/spark-examples-1.6.1-hadoop2.7.2.jar\
      $HDFS/data/lr_data 10

fi
