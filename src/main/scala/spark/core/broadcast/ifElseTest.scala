package spark.core.broadcast

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/7 14:39
 * @Version: 1.0
 * @Function: 测试 if else 更新变量
 */
object ifElseTest {
  def main(args: Array[String]): Unit = {
//    for (j <- 1 to 10) {
    var i = 3
    var s = "qwe"
    s = "asd"
      if (1 == 1) {
        i = 1
      } else {
        i = 2
      }
    println(i)
    println(s)
//    }
//    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
//    val sc: SparkContext = new SparkContext(conf)
//    sc.setLogLevel("WARN")
//    val value: Broadcast[Array[Int]] = sc.broadcast(Array(1, 2, 3, 4, 5))
//    val value1: Array[Int] = value.value
//    var j = 0
//    var s:String = ""
//    for (i <- value1) {
//      if ( j != value1.length-1) {
//        println(j + " 111 " + value1.length)
//        var s: String = ","
//        println(i + s)
//      } else {
//        println(j + "222" + value1.length)
//        var s: String = ""
//        println(i + s)
//      }
//      j += 1
//    }
  }
}
