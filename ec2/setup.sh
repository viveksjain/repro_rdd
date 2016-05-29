#!/bin/bash

source ~/scripts/common.sh

set -e

cp mapred-site.xml "$HADOOP_CONF_DIR/mapred-site.xml"
cp core-site.xml "$HADOOP_CONF_DIR/core-site.xml"
cp masters "$HADOOP_CONF_DIR/masters"
cp slaves "$HADOOP_CONF_DIR/slaves"

cp slaves "$SPARK_HOME/conf/slaves"
cp spark-defaults.conf "$SPARK_HOME/conf/spark-defaults.conf"

# Master stuff
if [ -f ~/scripts/is_master ]; then
  set -x
  mkdir -p timings/
  sbt package
  javac -classpath $HADOOP_INSTALL/share/hadoop/common/hadoop-common-2.7.2.jar:$HADOOP_INSTALL/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.2.jar LogisticRegression.java
  jar cf lr.jar LogisticRegression*.class
  javac -classpath $HADOOP_INSTALL/share/hadoop/common/hadoop-common-2.7.2.jar:$HADOOP_INSTALL/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.2.jar KMeans.java
  jar cf kmeans.jar KMeans*.class

  "$HADOOP_INSTALL/bin/hdfs" namenode -format -force
  "$HADOOP_INSTALL/sbin/start-dfs.sh"
  "$HADOOP_INSTALL/sbin/start-yarn.sh"
  "$SPARK_HOME/sbin/start-all.sh"
fi
