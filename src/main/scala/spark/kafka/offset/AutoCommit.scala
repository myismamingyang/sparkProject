package spark.kafka.offset

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
 * @Author: Mingyang Ma
 * @Date: 2023/12/26 20:53
 * @Version: 1.0
 * @Function:
 */
object AutoCommit {
  def main(args: Array[String]): Unit = {
    //1.准备环境
    val sparkConf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("./ckp")

    //2.从Kafka数据
    //准备连接参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "sparkstreaming",
      //earliest:表示如果有offset记录从offset记录开始消费,如果没有从最早的消息开始消费
      //latest:表示如果有offset记录从offset记录开始消费,如果没有从最后/最新的消息开始消费
      //none:表示如果有offset记录从offset记录开始消费,如果没有就报错
      "auto.offset.reset" -> "latest",
      "auto.commit.interval.ms"->"1000",//自动提交的时间间隔
      "enable.auto.commit" -> (true: java.lang.Boolean)//是否自动提交偏移量
    )
    //准备连接的参数:主题
    val topics = Array("spark_kafka")
    //使用工具类+参数连接Kafka
    //ConsumerRecord:表示从Kafka中消费到的一条条的消息
    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,//位置策略,使用源码中推荐的均匀一致的分配策略
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)//消费策略,使用源码中推荐的订阅模式
    )

    //获取记录中的详细信息
    val recordInfoDS: DStream[String] = kafkaDS.map(record => {
      val topic: String = record.topic()
      val partition: Int = record.partition()
      val offset: Long = record.offset()
      val key: String = record.key()
      val value: String = record.value()
      val info: String = s"""topic:${topic}, partition:${partition}, offset:${offset}, key:${key}, value:${value}"""
      info
    })
    //输出
    recordInfoDS.print()

    //启动并等待结束
    ssc.start() //流程序需要启动
    ssc.awaitTermination() //流程序会一直运行等待数据到来或手动停止
    ssc.stop(true, true) //是否停止sc,是否优雅停机

  }
}
/*
1.准备主题
/export/server/kafka/bin/kafka-topics.sh --list --zookeeper node1:2181
/export/server/kafka/bin/kafka-topics.sh --zookeeper node1:2181 --delete --topic spark_kafka
/export/server/kafka/bin/kafka-topics.sh --create --zookeeper node1:2181 --replication-factor 1 --partitions 3 --topic spark_kafka
/export/server/kafka/bin/kafka-topics.sh --list --zookeeper node1:2181
2.启动控制台生产者
/export/server/kafka/bin/kafka-console-producer.sh --broker-list node1:9092 --topic spark_kafka
3.启动程序
4.发送消息
 */
