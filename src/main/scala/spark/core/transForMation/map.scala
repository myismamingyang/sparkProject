package spark.core.transForMation

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/7 14:39
 * @Version: 1.0
 * @Function: 操作元素并返回
 */
object map {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd1: RDD[Int] = sc.parallelize(List(5, 6, 4))
    //parallelize 创建RDD --分区(分区数没有设置,使用默认值)
    rdd1.map(_ * 3).collect.foreach(println)
    // map 将RDD中每个value 进行共同的操作
    println("-------1-------")
    rdd1.map(_*3).foreach(println)
    sc.stop()
  }
}
