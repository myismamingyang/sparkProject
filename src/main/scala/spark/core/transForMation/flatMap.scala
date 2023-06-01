package spark.core.transForMation

import org.apache.spark.{SparkConf, SparkContext}

object flatMap {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd1 = sc.parallelize(Array("a b c", "d e f", "h i j"))
    val rdd2 = sc.parallelize(Array("a b c", "a b c", "h i j"))
    rdd1.flatMap(_.split(" ")).foreach(print)
    println()
    rdd2.flatMap(_.split(" ")).foreach(print)
    println()
    println("-----1-----")
    rdd1.flatMap(_.split(" ")).map((_,1)).foreach(print)
    println()
    rdd2.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).foreach(print)
  }
}
