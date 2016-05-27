import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import scala.util.Random

object KMeansDataGenerator {
    def main(args: Array[String]) {
        val recordsPerPartition = 10
        val numPartitions = 3
        val numFeatures = 9
        val outfile = "kmeans_data"
        val conf = new SparkConf().setAppName("K Means")
        val sc = new SparkContext(conf)

        val distData = sc.parallelize(Seq[Array[Double]](), numPartitions)
            .mapPartitions { _ => {
                (1 to recordsPerPartition).map{_ =>
                    Array.fill(numFeatures + 1){Random.nextDouble * 10}
                }.iterator
            }}
        distData.map{ point =>
            point.mkString(" ")
        }.saveAsTextFile(outfile)
    }
}