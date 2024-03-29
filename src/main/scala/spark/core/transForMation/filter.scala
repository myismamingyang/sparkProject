package spark.core.transForMation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/7 14:39
 * @Version: 1.0
 * @Function: 过滤函数
 */
object filter {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd1: RDD[Int] = sc.parallelize(List(5, 6, 4, 10, 23))
    rdd1.filter(_ % 2 == 0).foreach(println)
    // filter 将RDD中每个value 作操作判断符合值,只保留true
    println("-----1-----")
    rdd1.filter(_>=10).foreach(println)
    sc.stop()
  }
}
