package spark.core.transForMation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object set {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd1 = sc.parallelize(List(5, 6, 4, 3))
    val rdd2 = sc.parallelize(List(1, 2, 3, 4))
    println("-----并集不去重-----")
    rdd1.union(rdd2).collect().foreach(print)
    println()
    println("-----并集去重-----")
    rdd1.union(rdd2).distinct().collect().foreach(print)
    println()
    println("-----交集-----")
    rdd1.intersection(rdd2).collect().foreach(print)
    println()
    println("-----差集(左连接)-----")
    rdd1.subtract(rdd2).foreach(print)
    println()
    println("-----差集(左连接)-----")
    rdd2.subtract(rdd1).foreach(print)
    println()
    println("-----笛卡尔积-----")
    val rdds1 = sc.parallelize(List("jack", "tom"))//学生
    val rdds2 = sc.parallelize(List("java", "python", "scala"))//课程
    rdds1.cartesian(rdds2).foreach(print)
  }
}
