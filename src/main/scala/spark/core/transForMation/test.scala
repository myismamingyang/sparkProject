package spark.core.transForMation

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val values: RDD[String] = sc.textFile("data/input/words.txt")
    val result: RDD[(String, Int)] = values.filter(StringUtils.isNoneBlank(_))
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    result.foreach(println)

    println("-----------------------------------------")

    val strings: RDD[String] = sc.parallelize(List("dog", "dog","tiger", "lion", "cat", "panther", "eagle"), 2)
    strings.map((_,1)).reduceByKey(_+_).foreach(print)
  }
}
