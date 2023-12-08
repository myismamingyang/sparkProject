package spark.structuredstreaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/12/9 0:20
 * @Version: 1.0
 * @Function:
 */
object ForeachBatch_MySQL {
  def main(args: Array[String]): Unit = {
    //1.创建环境
    val spark: SparkSession = SparkSession.builder().appName("StructuredStreaming").master("local[*]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    //2.source
    //注意:读取流式数据时用spark.readStream
    val linesDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node1")
      .option("port", 9999)
      .load()
    linesDF.printSchema()

    //3.transformation-做WordCount
    val resultDS: Dataset[Row] = linesDF.as[String]
      .flatMap(_.split(" "))
      .groupBy("value")
      .count()
      .sort('count.desc)

    //4.输出
    //适用ForeachBatch自定义输出结果到MySQL
    resultDS.writeStream
      //.trigger(Trigger.ProcessingTime(0))
      .outputMode(OutputMode.Complete())
      .foreachBatch((batchDS: Dataset[Row], batchId: Long) => {
        println("-------------")
        println(s"batchId:${batchId}")
        println("-------------")
        //-1.自定义输出到控制台
        batchDS.show(false)
        //-2.自定义输出到MySQL
        //batchDS.write.jdbc(url,table,properties)
        batchDS.coalesce(1)
          .write
          .mode(SaveMode.Overwrite)
          .format("jdbc")
          //.option("driver", "com.mysql.cj.jdbc.Driver")//MySQL-8
          //.option("url", "jdbc:mysql://localhost:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true")//MySQL-8
          .option("url", "jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8")
          .option("user", "root")
          .option("password", "root")
          .option("dbtable", "bigdata.t_struct_words")
          .save()
      })
      .start()
      .awaitTermination()
  }
}
