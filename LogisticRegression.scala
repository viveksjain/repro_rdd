import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.Random
import scala.math._
import language.postfixOps

object LogisticRegression {
    def main(args: Array[String]) {
        val ITERATIONS = 10
        val logFile = "spark_log"
        val conf = new SparkConf().setAppName("Logistic Regression")
        val sc = new SparkContext(conf)

        var numX = 0
        // Every line is a separate point. Within each line, numbers should be
        // separated by spaces and the last number is either 1 or -1 (the
        // classification)
        val points = sc.textFile("logistic_regression_data").map{ p =>
            val arr = p.split(" ").map(x => x.toDouble)
            numX = arr.length - 1
            new Point(arr.slice(0, numX), arr(arr.length - 1).toInt)
        }.persist()

        var w: Array[Double] = Array.fill(numX){Random.nextInt(100)}
        println(w.mkString(" "))
        for (i <- 1 to ITERATIONS) {
            val gradient: Array[Double] = points.map{ p =>
                mult(p.x, (1 / (1 + exp(-p.y * (dot(w, p.x)))) - 1) * p.y)
            }.reduce((a,b) => add(a, b))
            w = subtract(w, gradient)
        }
    }

    def dot(arr1: Array[Double], arr2: Array[Double]) = {
        ((arr1 zip arr2) map { case (x, y) => x * y }) sum
    }

    // arr1 - arr2
    def subtract(arr1: Array[Double], arr2: Array[Double]) = {
        (arr1 zip arr2) map { case (x, y) => x - y }
    }

    def mult(arr: Array[Double], n: Double) = {
        arr.map(x => x * n)
    }

    def add(arr1: Array[Double], arr2: Array[Double]) = {
        (arr1 zip arr2) map { case (x, y) => x + y }
    }
}

class Point(xarg: Array[Double], yarg: Int) {
    var x: Array[Double] = xarg
    var y: Int = yarg
}