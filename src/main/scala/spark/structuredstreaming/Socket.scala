package spark.structuredstreaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/12/7 22:12
 * @Version: 1.0
 * @Function:
 */
object Socket {
  def main(args: Array[String]): Unit = {
    //1.创建环境
    val spark: SparkSession = SparkSession.builder()
      .appName("StructuredStreaming-Socket")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "2") //默认是200,本地测试给少一点
      //还有很多其他的参数设置项目中进行设置,这里简单一点
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
    /*
    root
     |-- value: string (nullable = true)
     */
    //注意:流模式下,不能直接show,会报如下错误
    //Queries with streaming sources must be executed with writeStream.start();
    //linesDF.show(false)

    //3.transformation-做WordCount
    val resultDS: Dataset[Row] = linesDF.as[String]
      .flatMap(_.split(" "))
      .groupBy("value")
      .count()
    //.sort('count.desc)

    //4.输出
    resultDS
      .writeStream
      //- append:默认的追加模式,将新的数据输出!只支持简单查询,如果涉及的聚合就不支持了
      //- complete:完整模式,将完整的数据输出,支持聚合和排序
      //- update:更新模式,将有变化的数据输出,支持聚合但不支持排序,如果没有聚合就和append一样
      //.outputMode(OutputMode.Append())
      //.outputMode(OutputMode.Complete())
      .outputMode(OutputMode.Update())
      .format("console") //往控制台输出
      .option("numRows", "10") //打印多少条数据，默认为20条
      .option("truncate", "false") //如果某列值字符串太长是否截取，默认为true
      //5.启动并等待停止
      .start()
      .awaitTermination()
  }
}
