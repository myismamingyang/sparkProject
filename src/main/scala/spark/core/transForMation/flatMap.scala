package spark.core.transForMation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/7 14:39
 * @Version: 1.0
 * @Function: 压扁聚合
 */
object flatMap {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
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
    sc.stop()
  }
}
