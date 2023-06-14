package spark.streaming.JDBC.mysql

import org.apache.commons.lang3.StringUtils

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.mortbay.util.StringUtil

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/14 13:56
 * @Version: 1.0
 * @Function: 使用sparkStreaming插入mysql数据
 */
object insertTable {
  def main(args: Array[String]): Unit = {
    //1.创建环境
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName.stripSuffix("$")).setMaster("local[*]")
      .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("./ckp")

    val word: RDD[String] = sc.textFile("data/input/person.txt")

    val wordValue: RDD[Array[String]] = word.map(_.split(" "))
    for (e <- wordValue){
      println(e.mkString)
    }
  }
    //RDD[Array[(s)(s)(s))]]
}