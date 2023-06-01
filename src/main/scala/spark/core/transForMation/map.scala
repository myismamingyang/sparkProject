package spark.core.transForMation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object map {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd1: RDD[Int] = sc.parallelize(List(5, 6, 4, 7, 3, 8, 2, 9, 1, 10))
    val values: RDD[Int] = rdd1.map(_ * 3).collect
    values.foreach(println)
  }
}
