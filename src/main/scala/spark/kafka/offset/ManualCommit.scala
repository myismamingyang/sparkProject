package spark.kafka.offset

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import shapeless.record

/**
 * @Author: Mingyang Ma
 * @Date: 2023/12/26 20:55
 * @Version: 1.0
 * @Function:
 */
object ManualCommit {
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
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)//是否自动提交偏移量
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
    //注意:如果要手动提交,需要确定提交的时机
    //1.每隔多久提交一次offset,这个就和自动提交一样了,手动提交没必要这样写
    //2.接收一条提交一次,可以但是太频繁了
    //3.接收一批提交一次,比较合适
    //注意:在SparkStreaming中,DStream底层的RDD就是一批批的数据
    //所以可以对DStream底层的RDD进行操作,每处理一个RDD就表示处理了一批数据,就可以提交了
    kafkaDS.foreachRDD(rdd=>{
      if (!rdd.isEmpty()){
        //-1.处理该批次的数据/该RDD中的数据--我们这里就直接使用输出表示处理完了
        rdd.foreach(r=>{
          println(s"topic:${r.topic()},partition:${r.partition()},offset:${r.offset()},key:${r.key()},value:${r.value()}")
        })
        //val array: Array[ConsumerRecord[String, String]] = rdd.collect()

        //-2.处理好之后提交offset
        //原本应该将RDD中的主题/分区/偏移量等信息提取处理再提交,但是这样做麻烦
        //所以官方提供一种方式,直接将RDD转为Array[OffsetRange]就可以直接提交了
        //offsetRanges里面存放的就是主题/分区/偏移量等信息
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //提交主题/分区/偏移量等信息
        kafkaDS.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        println("主题/分区/偏移量等信息已手动提交到Checkpoint/默认主题中")
      }
    })

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