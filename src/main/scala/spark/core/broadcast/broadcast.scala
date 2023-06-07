package spark.core.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/7 14:39
 * @Version: 1.0
 * @Function: 广播变量
 */
object broadcast {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val value: Broadcast[Array[Int]] = sc.broadcast(Array(1, 2, 3, 4, 5))
    val value1: Array[Int] = value.value
    var j = 1
    var s: String = ""
//    for (i <- value1) {
//      if (value1.length != j) {
//        s = ","
//        //print(i + s)
//      } else {
//        s = ""
//        //print(i + s)
//      }
//      print(i + s)
//      j += 1
//    }
    sc.stop()
  }
}
