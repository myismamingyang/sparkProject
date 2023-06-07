package spark.core.transForMation

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/7 14:39
 * @Version: 1.0
 * @Function: 获取 key,value 值
 */
object keysValues {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val values: RDD[String] = sc.parallelize(List("dog", "dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    val value: RDD[(Int, String)] = values.map(x => (x.length, x))
    println("----------value.foreach-----")
    value.collect().foreach(print)
    println()
    println("----------value.keys.-----")
    value.keys.collect().foreach(print)
    println()
    println("----------value.values.-----")
    value.values.collect().foreach(print)
    println()
    println("-----for------")
    val strings: Array[String] = value.values.collect()
    for (i <- 0 to strings.length - 1) {
      print(i + " ")
      println(strings(i))
    }
    println("-----for-plus-----")
    var j = 0
    for (i <- strings) {
      println(j + " " + i)
      j += 1
    }
    println()
    println("----------value.map.reduceByKey-----")
    value.values.map((_, 1)).reduceByKey(_ + _).foreach(print)
    sc.stop()
  }
}
