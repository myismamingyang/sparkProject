package spark.kafka.InternetOfThings

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/12/13 23:53
 * @Version: 1.0
 * @Function:
 */
object InternetOfThingsAnalysis {
  def main(args: Array[String]): Unit = {
    //0.创建环境
    //因为StructuredStreaming基于SparkSQL的且编程API/数据抽象是DataFrame/DataSet,所以这里创建SparkSession即可
    val spark: SparkSession = SparkSession.builder().appName("sparksql").master("local[*]")
      .config("spark.sql.shuffle.partitions", "4") //本次测试时将分区数设置小一点,实际开发中可以根据集群规模调整大小,默认200
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._
    import org.apache.spark.sql.functions._
    //1.加载数据-kafka-iotTopic
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092")
      .option("subscribe", "iotTopic")
      .load()
    val valueDS: Dataset[String] = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]
    //{"device":"device_30","deviceType":"kafka","signal":77.0,"time":1610158709534}

    //2.处理数据
    //需求:统计信号强度>30的各种设备类型对应的数量和平均信号强度
    //解析json(也就是增加schema:字段名和类型)
    //方式1:fastJson/Gson等工具包,后续案例中使用
    //方式2:使用SparkSQL的内置函数,当前案例使用
    val schemaDF: DataFrame = valueDS.filter(StringUtils.isNotBlank(_))
      .select(
        get_json_object($"value", "$.device").as("device_id"),
        get_json_object($"value", "$.deviceType").as("deviceType"),
        get_json_object($"value", "$.signal").cast(DoubleType).as("signal")
      )
    //需求:统计信号强度>30的各种设备类型对应的数量和平均信号强度
    //====SQL
    schemaDF.createOrReplaceTempView("t_iot")
    val sql: String =
      """
        |select deviceType,count(*) as counts,avg(signal) as avgsignal
        |from t_iot
        |where signal > 30
        |group by deviceType
        |""".stripMargin
    val result1: DataFrame = spark.sql(sql)
    //====DSL
    val result2: DataFrame = schemaDF.filter('signal > 30)
      .groupBy('deviceType)
      .agg(
        count('device_id) as "counts",
        avg('signal) as "avgsignal"
      )
    //3.输出结果-控制台
    result1.writeStream
      .format("console")
      .outputMode("complete")
      //.option("truncate", false)
      .start()
    //.awaitTermination()
    //4.启动并等待结束
    result2.writeStream
      .format("console")
      .outputMode("complete")
      //.trigger(Trigger.ProcessingTime(0))
      //.option("truncate", false)
      .start()
      .awaitTermination()
    //5.关闭资源
    spark.stop()
  }
}
