#!/bin/bash
$SPARK_HOME/bin/spark-submit --class SparkHdfsLR \
  --master $SPARK_MASTER target/scala-2.10/data-generator_2.10-1.0.jar \
  $SPARK_MASTER $HDFS/data/lr_data 10
