package spark.kafka.realTimeETL

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/12/12 23:31
 * @Version: 1.0
 * @Function:
 * 从SimulertBasestasjonLog消费数据
 * -->使用StructuredStreaming进行ETL
 * -->将ETL的结果写入到BasestasjonLogETL2Store
 */
object BasestasjonLogETL2Store {
  def main(args: Array[String]): Unit = {
    //TODO 0.创建环境
    //因为StructuredStreaming基于SparkSQL的且编程API/数据抽象是DataFrame/DataSet,所以这里创建SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("BasestasjonLogETL2Store")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "3") //本次测试时将分区数设置小一点,实际开发中可以根据集群规模调整大小,默认200
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    //TODO 1.加载数据-kafka-stationTopic
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092")
      .option("subscribe", "SimulertBasestasjonLog")
      .load()
    val valueDS: Dataset[String] = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]

    //TODO 2.处理数据-ETL-过滤出success的数据
    val etlResult: Dataset[String] = valueDS.filter(_.contains("success"))

    //TODO 3.输出结果-kafka-etlTopic
    etlResult.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092")
      .option("topic", "BasestasjonLogETL2Store")
      .option("checkpointLocation", "./ckp")
      //TODO 4.启动并等待结束
      .start()
      .awaitTermination()


    //TODO 5.关闭资源
    spark.stop()
  }
  //0.kafka准备好
  //1.启动数据模拟程序 SimulertBasestasjonLog
  //2.启动控制台消费者方便观察 主题:SimulertBasestasjonLog
  //3.启动 BasestasjonLogETL2Store 主题:SimulertBasestasjonLog
}
