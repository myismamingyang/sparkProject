package spark.core.transForMation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object firstTakeTop {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val rdd: RDD[Int] = sc.parallelize(List(3, 6, 1, 2, 4, 5))
    println(rdd.first())
    rdd.take(3).foreach(print)
    println()
    rdd.top(3).foreach(print)
    sc.stop()
  }
}
