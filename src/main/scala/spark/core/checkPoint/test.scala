package spark.core.checkPoint

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/7 14:39
 * @Version: 1.0
 * @Function: 测试
 */
object test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    sc.stop()
  }
}
