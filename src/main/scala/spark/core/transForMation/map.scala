package spark.core.transForMation

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD

import org.apache.spark.{SparkConf, SparkContext}

object map {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd1: RDD[Int] = sc.parallelize(List(5, 6, 4))
    rdd1.map(_ * 3).collect.foreach(println)
    println("-------1-------")
    rdd1.map(_*3).foreach(println)
  }
}
