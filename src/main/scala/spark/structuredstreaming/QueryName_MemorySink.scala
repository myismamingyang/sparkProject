package spark.structuredstreaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/12/9 0:03
 * @Version: 1.0
 * @Function:
 */
object QueryName_MemorySink {
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
    //将上述的WordCount结果表(无界表保存到内存中),起个名字叫t_result
    resultDS
      .writeStream
      .outputMode(OutputMode.Complete())
      .format("memory")
      .queryName("t_result")
      .start()
    //.awaitTermination()//后续要对无界表中的数据进行查询,所以该行阻塞等待需要注掉

    while(true){
      Thread.sleep(3000)
      spark.sql("select * from t_result").show(false)
    }
  }
}
