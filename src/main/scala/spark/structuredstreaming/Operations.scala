package spark.structuredstreaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/12/8 21:04
 * @Version: 1.0
 * @Function:
 */
object Operations {
  def main(args: Array[String]): Unit = {
    //1.创建环境
    val spark: SparkSession = SparkSession.builder()
      .appName("StructuredStreaming-Operations")
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
    linesDF.printSchema()

    //3.transformation-做WordCount
    val wordsDS: Dataset[String] = linesDF.as[String]
      .flatMap(_.split(" "))

    val resultDS: Dataset[Row] = wordsDS
      .groupBy("value")
      .count()
      .sort('count.desc)

    wordsDS.createOrReplaceTempView("t_words")
    val sql: String =
      """
        |select value as word,count(*) as counts
        |from t_words
        |group by word
        |order by counts desc
        |""".stripMargin
    val resultDS2: DataFrame = spark.sql(sql)

    //4.输出
    println(resultDS.isStreaming)
    resultDS
      .writeStream
      .outputMode("complete")
      .format("console")
      //5.启动并等待停止
      .start()
    //.awaitTermination()//注意:因为后面还有代码需要执行,所以这里的阻塞等待需要注掉

    println(resultDS2.isStreaming)
    resultDS2
      .writeStream
      .outputMode("complete")
      .format("console") //往控制台输出
      //5.启动并等待停止
      .start()
      .awaitTermination()
  }
}
