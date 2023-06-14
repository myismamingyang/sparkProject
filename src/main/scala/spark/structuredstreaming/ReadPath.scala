package spark.structuredstreaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/12/7 23:46
 * @Version: 1.0
 * @Function:
 */
object ReadPath {
  def main(args: Array[String]): Unit = {
    //1.创建环境
    val spark: SparkSession = SparkSession.builder()
      .appName("StructuredStreaming-ReadPath")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //2.source
    val csvSchema: StructType = new StructType()
      .add("name", StringType, nullable = true)
      .add("age", IntegerType, nullable = true)
      .add("hobby", StringType, nullable = true)

    val fileDF: DataFrame = spark.readStream
      .option("sep", ";")
      .option("header", "false")
      .schema(csvSchema)
      .csv("data/input/persons")// Equivalent to format("csv").load("/path/to/directory")

    //3.输出
    fileDF
      .writeStream
      .outputMode("append")//append
      .format("console")
      .option("numRows", "30")
      //打印多少条数据，默认为20条
      //(参数小于20 数据条数大于20 也会打印20条)(只有参数大于20才有实用效果)
      //only showing top 30 rows
      //同一时间新添加文件会放在一个批次(打印参数条),若当前批次超过参数也只打印参数条
      .option("truncate",true)
      .start()
      .awaitTermination()
  }
}
