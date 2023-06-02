package spark.core.transForMation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object mapValues {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val values: RDD[(Int, Int)] = sc.parallelize(List((1, 10), (2, 20), (3, 30)))
    values.mapValues(_ * 2).collect().foreach(println)
    sc.stop()
  }
}
