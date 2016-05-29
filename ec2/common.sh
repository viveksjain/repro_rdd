#!/bin/bash

cd "$(dirname "$0")"

export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64
export HADOOP_INSTALL=/home/ubuntu/hadoop
export HADOOP_MAPRED_HOME=$HADOOP_INSTALL
export HADOOP_COMMON_HOME=$HADOOP_INSTALL
export HADOOP_HDFS_HOME=$HADOOP_INSTALL
export YARN_HOME=$HADOOP_INSTALL
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_INSTALL/lib/native
export HADOOP_OPTS="-Djava.library.path=$HADOOP_INSTALL/lib"
export HADOOP_CONF_DIR=$HADOOP_INSTALL/etc/hadoop
export HADOOP_DATANODE_OPTS="-Xmx12g"

export SPARK_HOME=/home/ubuntu/spark

SPARK_MASTER=spark://`hostname`:7077
HDFS=hdfs://`hostname`:54310
