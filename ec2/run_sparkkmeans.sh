#!/bin/bash
$SPARK_HOME/bin/spark-submit --class SparkKMeans \
  --master $SPARK_MASTER target/scala-2.10/data-generator_2.10-1.0.jar \
  $SPARK_MASTER $HDFS/data/kmeans_data 10 10
