package spark.core.transForMation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/7 14:39
 * @Version: 1.0
 * @Function: kv类型数据 转为map
 */
object collectAsMap {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val value: RDD[(String, Int)] = sc.parallelize(List(("a", 1), ("b", 2), ("b", 3)))
    value.collectAsMap().foreach(println)
    // key重复会造成数据丢失
    sc.stop()
  }
}
