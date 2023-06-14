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

    val words: RDD[Array[String]] = word.map(_.split(" "))

    words.foreachPartition(iter => {
      val conn: Connection = DriverManager.getConnection("jdbc:mysql://node3:3306/testLibrary?characterEncoding=UTF-8", "root", "MMYqq123")
      val sql: String = "INSERT INTO `scala_JDBC_test` (`id`, `commit_log`, `commit_time`) VALUES (?, ?, ?);"
      val ps: PreparedStatement = conn.prepareStatement(sql) //获取预编译语句对象
      iter.foreach(t => {
        val id: Int = t(0).toInt
        val commit_log: String = t(1)
        val commit_time: String = t(2)
        ps.setInt(1, id)
        ps.setString(2, commit_log)
        ps.setString(3, commit_time)
        ps.addBatch()
      })
      ps.executeBatch()

      ps.close()
      conn.close()
    })
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
//RDD[Array[(s)(s)(s))]]
