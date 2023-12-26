package spark.kafka.offset

import java.sql.{DriverManager, ResultSet}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @Author: Mingyang Ma
 * @Date: 2023/12/26 20:56
 * @Version: 1.0
 * @Function:
 */
object ManualCommit2MySQL {
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

    //TODO 注意:连接kafka之前应该要先从mysql中获取offset信息
    //Map[主题分区, offset]
    val offsetMap: mutable.Map[TopicPartition, Long] = OffsetUtil.getOffsetMap("sparkstreaming","spark_kafka")
    val kafkaDS: InputDStream[ConsumerRecord[String, String]] =  if(offsetMap.size > 0){
      println("MySQL中存储了offset记录,从记录处开始消费")
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,//位置策略,使用源码中推荐的均匀一致的分配策略
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams,offsetMap)//消费策略,使用源码中推荐的订阅模式
      )
    }else{
      println("MySQL中没有存储offset记录,从latest处开始消费")
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,//位置策略,使用源码中推荐的均匀一致的分配策略
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)//消费策略,使用源码中推荐的订阅模式
      )
    }

    kafkaDS.foreachRDD(rdd=>{
      if (!rdd.isEmpty()){
        //-1.处理该批次的数据/该RDD中的数据--我们这里就直接使用输出表示处理完了
        rdd.foreach(r=>{
          println(s"主题:${r.topic()},分区:${r.partition()},偏移量:${r.offset()},key:${r.key()},value:${r.value()}")
        })
        //val array: Array[ConsumerRecord[String, String]] = rdd.collect()

        //-2.处理好之后提交offset
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //提交主题/分区/偏移量等信息
        //kafkaDS.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        //TODO ok
        OffsetUtil.saveOffsetRanges("sparkstreaming",offsetRanges)
        println("主题/分区/偏移量等信息已手动提交到MySQL中了")
      }
    })

    //启动并等待结束
    ssc.start() //流程序需要启动
    ssc.awaitTermination() //流程序会一直运行等待数据到来或手动停止
    ssc.stop(true, true) //是否停止sc,是否优雅停机
  }
  /*
  手动维护offset的工具类
  首先在MySQL创建如下表
    CREATE TABLE `t_offset` (
      `topic` varchar(255) NOT NULL,
      `partition` int(11) NOT NULL,
      `groupid` varchar(255) NOT NULL,
      `offset` bigint(20) DEFAULT NULL,
      PRIMARY KEY (`topic`,`partition`,`groupid`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
   */
  object OffsetUtil {
    //1.将偏移量保存到数据库
    def saveOffsetRanges(groupid: String, offsetRange: Array[OffsetRange]) = {
      val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8", "root", "root")
      //replace into表示之前有就替换,没有就插入
      val ps = connection.prepareStatement("replace into t_offset (`topic`, `partition`, `groupid`, `offset`) values(?,?,?,?)")
      for (o <- offsetRange) {
        ps.setString(1, o.topic)
        ps.setInt(2, o.partition)
        ps.setString(3, groupid)
        ps.setLong(4, o.untilOffset)
        ps.executeUpdate()
      }
      ps.close()
      connection.close()
    }

    //2.从数据库读取偏移量Map(主题分区,offset)
    def getOffsetMap(groupid: String, topic: String) = {
      val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8", "root", "root")
      val ps = connection.prepareStatement("select * from t_offset where groupid=? and topic=?")
      ps.setString(1, groupid)
      ps.setString(2, topic)
      val rs: ResultSet = ps.executeQuery()
      //Map(主题分区,offset)
      val offsetMap = mutable.Map[TopicPartition, Long]()
      while (rs.next()) {
        offsetMap += new TopicPartition(rs.getString("topic"), rs.getInt("partition")) -> rs.getLong("offset")
      }
      rs.close()
      ps.close()
      connection.close()
      offsetMap
    }
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
