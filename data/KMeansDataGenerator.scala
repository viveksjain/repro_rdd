import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import scala.util.Random

object KMeansDataGenerator {
    def main(args: Array[String]) {
        if (args.length < 5) {
            System.err.println("Usage: KMeansDataGenerator <master> <file> <numFeatures> <numPartitions> <recordsPerPartition>")
            System.exit(1)
        }

        val sc = new SparkContext(args(0), "KMeansDataGenerator")
        val outfile = args(1)
        val numFeatures = args(2).toInt
        val numPartitions = args(3).toInt
        val recordsPerPartition = args(4).toInt        

        val distData = sc.parallelize(Seq[Array[Double]](), numPartitions)
            .mapPartitions { _ => {
                (1 to recordsPerPartition).map{_ =>
                    Array.fill(numFeatures){Random.nextDouble * 10}
                }.iterator
            }}
        distData.map{ point =>
            point.mkString(" ")
        }.saveAsTextFile(outfile)
    }
}