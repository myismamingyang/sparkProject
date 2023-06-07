package spark.core.getData

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/7 14:39
 * @Version: 1.0
 * @Function: 加载外部数据源
 */
object sequenceAndObject {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val result: RDD[(String, Int)] = sc.textFile("data/input/words.txt")
      .flatMap(_.split(" "))
      .map((_, 1)) //_表示每一个单词
      .reduceByKey(_ + _)

    result.coalesce(1).saveAsSequenceFile("data/output/sequence") //保存为序列化文件
    result.coalesce(1).saveAsObjectFile("data/output/object") //保存为对象文件

    val rdd1: RDD[(String, Int)] = sc.sequenceFile("data/output/sequence") //读取sequenceFile
    val rdd2: RDD[(String, Int)] = sc.objectFile("data/output/object") //读取objectFile

    rdd1.foreach(println)
    rdd2.foreach(println)

    sc.stop()
  }
}
