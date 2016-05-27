import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import scala.util.Random

object LRDataGenerator {
    def main(args: Array[String]) {
        val recordsPerPartition = 10
        val numPartitions = 3
        val numFeatures = 9
        val outfile = "lr_data"
        val conf = new SparkConf().setAppName("Logistic Regression")
        val sc = new SparkContext(conf)

        val distData = sc.parallelize(Seq[Array[Double]](), numPartitions)
            .mapPartitions { _ => {
                (1 to recordsPerPartition).map{_ =>
                    var arr: Array[Double] = Array.fill(numFeatures + 1){Random.nextDouble * 5}
                    arr(0) = if (Random.nextBoolean) 1 else -1
                    arr
                }.iterator
            }}
        distData.map{ point =>
            point.mkString(" ")
        }.saveAsTextFile(outfile)
    }
}