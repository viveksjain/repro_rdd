import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import scala.util.Random

object LRDataGenerator {
    def main(args: Array[String]) {
        if (args.length < 5) {
            System.err.println("Usage: LRDataGenerator <master> <file> <numFeatures> <numPartitions> <recordsPerPartition>")
            System.exit(1)
        }

        val sc = new SparkContext(args(0), "LRDataGenerator")
        val outfile = args(1)
        val numFeatures = args(2).toInt
        val numPartitions = args(3).toInt
        val recordsPerPartition = args(4).toInt

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