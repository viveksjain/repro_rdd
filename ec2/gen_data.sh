if [ "$#" -ne 2 ]; then
  echo "Usage: gen_data.sh <num slaves>"
  exit 1
fi

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
$SPARK_HOME/bin/spark-submit --class "KMeansDataGenerator" \
  --master $SPARK_MASTER \
  target/scala-2.10/data-generator_2.10-1.0.jar \
    $SPARK_MASTER $HDFS/data/kmeans_data 10 $1 400000000 # TODO size
$SPARK_HOME/bin/spark-submit --class "LRDataGenerator" \
  --master $SPARK_MASTER \
  target/scala-2.10/data-generator_2.10-1.0.jar \
    $SPARK_MASTER $HDFS/data/lr_data 10 $1 400000000 # TODO 4000000000
