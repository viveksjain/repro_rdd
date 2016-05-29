#!/bin/bash
source ~/scripts/common.sh

if $($HADOOP_INSTALL/bin/hdfs dfs -test -d /data); then
  $HADOOP_INSTALL/bin/hdfs dfs -rm -r /data
fi
if $($HADOOP_INSTALL/bin/hdfs dfs -test -d /output); then
  $HADOOP_INSTALL/bin/hdfs dfs -rm -r /output
fi
"$SPARK_HOME/sbin/stop-all.sh"
"$HADOOP_INSTALL/sbin/stop-dfs.sh"
"$HADOOP_INSTALL/sbin/stop-yarn.sh"
