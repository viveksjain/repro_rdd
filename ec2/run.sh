#!/bin/bash

# Script to run on EC2 nodes.

set -e

function setup_env {
  export HADOOP_INSTALL=$1
  export HADOOP_MAPRED_HOME=$HADOOP_INSTALL
  export HADOOP_COMMON_HOME=$HADOOP_INSTALL
  export HADOOP_HDFS_HOME=$HADOOP_INSTALL
  export YRAN_HOME=$HADOOP_INSTALL
  export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_INSTALL/lib/native
  export HADOOP_OPTS="-Djava.library.path=$HADOOP_INSTALL/lib"

  export SPARK_HOME=$2

  if [[ $3 ]]; then
    export MESOS_HOME=$3
  fi
}

function copy_conf {
  export HADOOP_CONF_DIR=$1
  cp mapred-site.xml "$HADOOP_CONF_DIR/mapred-site.xml"
  cp core-site.xml "$HADOOP_CONF_DIR/core-site.xml"
  cp masters "$HADOOP_CONF_DIR/masters"
  cp slaves "$HADOOP_CONF_DIR/slaves"

  cp slaves "$SPARK_HOME/conf/slaves"

  if [[ $MESOS_HOME ]]; then
    cp mesos.conf "$MESOS_HOME/var/mesos/deploy/mesos.conf"
    cp masters "$MESOS_HOME/var/mesos/deploy/masters"
    cp slaves "$MESOS_HOME/var/mesos/deploy/slaves"
  fi
}

cd "$(dirname "$0")"

setup_env /home/ubuntu/hadoop /home/ubuntu/spark
copy_conf "$HADOOP_INSTALL/etc/hadoop"

# Master stuff
if [ -f ~/scripts/is_master ]; then
  set -x
  "$HADOOP_INSTALL/sbin/start-dfs.sh"
  "$HADOOP_INSTALL/sbin/start-yarn.sh"
  "$HADOOP_INSTALL/bin/hdfs" namenode -format -force
  "$SPARK_HOME/sbin/start-all.sh"

  SPARK_MASTER=spark://`hostname`:7077
  HDFS="hdfs://`hostname`:54310"
  
  # HDFS won't let you overwrite files/directories
  if $($HADOOP_INSTALL/bin/hdfs dfs -test -d /data); then
    $HADOOP_INSTALL/bin/hdfs dfs -rm -r /data
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

  # TODO clear IO buffers before each
  # Run ML algorithms
  $SPARK_HOME/bin/spark-submit --class org.apache.spark.examples.SparkKMeans \
    --master $SPARK_MASTER \
    $SPARK_HOME/examples/target/scala-2.10/spark-examples-1.6.1-hadoop2.7.2.jar\
      $HDFS/data/kmeans_data 3 0.5
  $SPARK_HOME/bin/spark-submit --class org.apache.spark.examples.SparkHdfsLR \
    --master $SPARK_MASTER \
    $SPARK_HOME/examples/target/scala-2.10/spark-examples-1.6.1-hadoop2.7.2.jar\
      $HDFS/data/lr_data 10

  $HADOOP_INSTALL/bin/hdfs dfs -rm -r /data
  "$SPARK_HOME/sbin/stop-all.sh"
  "$HADOOP_INSTALL/sbin/stop-dfs.sh"
  "$HADOOP_INSTALL/sbin/stop-yarn.sh"
  set +x
fi

setup_env /home/ubuntu/hadoop-0.20.205.0 /home/ubuntu/spark-0.5.0 /home/ubuntu/mesos-0.9.0
copy_conf "$HADOOP_INSTALL/conf"
# Use other core-site file, so different Hadoop versions aren't using same temp dir.
cp core-site2.xml "$HADOOP_CONF_DIR/core-site.xml"

if [ -f ~/scripts/is_master ]; then
  set -x
  "$MESOS_HOME/sbin/mesos-start-cluster.sh"
  "$HADOOP_INSTALL/bin/hadoop" namenode -format -force
  "$HADOOP_INSTALL/bin/start-all.sh"
  # TODO everything else
fi
