package spark.core.transForMation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object filter {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd1: RDD[Int] = sc.parallelize(List(5, 6, 4, 10, 23))
    rdd1.filter(_ % 2 == 0).foreach(println)
    println("-----1-----")
    rdd1.filter(_>=10).foreach(println)
    sc.stop()
  }
}
