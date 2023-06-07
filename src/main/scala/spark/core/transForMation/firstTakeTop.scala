package spark.core.transForMation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/7 14:39
 * @Version: 1.0
 * @Function: 排序显示函数; 第一 前三 从大到小
 */
object firstTakeTop {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val rdd: RDD[Int] = sc.parallelize(List(3, 6, 1, 2, 4, 5))
    println(rdd.first())
    rdd.take(3).foreach(print)
    println()
    rdd.top(3).foreach(print)
    sc.stop()
  }
}
