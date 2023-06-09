package spark.core.transForMation

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.SizeBasedWindowFunction.n
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/7 14:39
 * @Version: 1.0
 * @Function: set 交集,并集,笛卡尔积...
 *            distinct 去重
 */
object setAndDistinct {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // ------------------------set---------------------------
    val rdd1 = sc.parallelize(List(5, 6, 4, 3))
    val rdd2 = sc.parallelize(List(1, 2, 3, 4))
    println("----------并集不去重-----")
    rdd1.union(rdd2).collect().foreach(print)
    println()
    println("----------并集去重-----")
    rdd1.union(rdd2).distinct().collect().foreach(print)
    println()
    println("----------交集-----")
    rdd1.intersection(rdd2).collect().foreach(print)
    println()
    println("----------差集(左连接)-----")
    rdd1.subtract(rdd2).foreach(print)
    println()
    println("----------差集(左连接)-----")
    rdd2.subtract(rdd1).foreach(print)
    println()
    println("----------笛卡尔积-----")
    val rdds1 = sc.parallelize(List("jack", "tom"))//学生
    val rdds2 = sc.parallelize(List("java", "python", "scala"))//课程
    rdds1.cartesian(rdds2).foreach(print)

    //  -----------------------distinct------------------------
    val rddDistinct = sc.parallelize(Array(1,2,3,4,5,5,6,7,8,1,2,3,4),3)
    println()
    println("----------rddDistinct-----")
    rddDistinct.foreach(print)
    println('\n' + "----------rddDistinct-reduceByKey-----")
    rddDistinct.map((_,1)).reduceByKey(_+_).foreach(print)
    println()
    println("----------rddDistinct-distinct-----")
    rddDistinct.distinct.collect.foreach(print)
    sc.stop()
  }
}
