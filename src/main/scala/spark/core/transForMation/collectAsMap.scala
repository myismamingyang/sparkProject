package spark.core.transForMation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object collectAsMap {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val value: RDD[(String, Int)] = sc.parallelize(List(("a", 1), ("b", 2), ("b", 3)))
    value.collectAsMap().foreach(println)
    // key重复会造成数据丢失
  }
}
