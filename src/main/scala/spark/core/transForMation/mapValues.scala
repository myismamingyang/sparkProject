package spark.core.transForMation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/7 14:39
 * @Version: 1.0
 * @Function: 操作kv种 value值,key不变,value返回
 */
object mapValues {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val values: RDD[(Int, Int)] = sc.parallelize(List((1, 10), (2, 20), (3, 30)))
    values.mapValues(_ * 2).collect().foreach(println)
    sc.stop()
  }
}
