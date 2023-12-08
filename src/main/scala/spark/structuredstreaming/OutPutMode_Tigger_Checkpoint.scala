package spark.structuredstreaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/12/8 23:41
 * @Version: 1.0
 * @Function:
 */
object OutPutMode_Tigger_Checkpoint {
  def main(args: Array[String]): Unit = {
    //1.创建环境
    val spark: SparkSession = SparkSession.builder()
      .appName("OutPutMode_Tigger_Checkpoint")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    //2.source
    val linesDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node1")
      .option("port", 9999)
      .load()

    //3.transformation-做WordCount
    val resultDS: Dataset[Row] = linesDF.as[String]
      .flatMap(_.split(" "))
      .groupBy("value")
      .count()
    //.sort('count.desc)

    //4.输出
    resultDS
      .writeStream
      .format("console")
      //append:只输出无界表中的新的数据,只适用于简单查询
      //complete:输出无界表中所有数据,必须包含聚合才可以用
      //update:只输出无界表中有更新的数据,不支持排序
      //.outputMode(OutputMode.Complete())
      //.outputMode(OutputMode.Append())//Append output mode not supported when there are streaming aggregations
      .outputMode(OutputMode.Update())//Sorting is not supported
      //0, the query will run as fast as possible.
      .trigger(Trigger.ProcessingTime("0 seconds"))
      .option("checkpointLocation", "./ckp"+System.currentTimeMillis())
      //5.启动并等待停止
      .start()
      .awaitTermination()
  }
}
