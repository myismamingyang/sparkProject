package spark.kafka.InternetOfThings

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.json4s.jackson.Json

import java.util.{Properties, Random}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/12/13 23:44
 * @Version: 1.0
 * @Function:
 */
object InternetOfThingsLog {
  def main(args: Array[String]): Unit = {
    // 发送Kafka Topic
    val props = new Properties()
    props.put("bootstrap.servers", "node1:9092")
    props.put("acks", "1")
    props.put("retries", "3")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](props)

    val deviceTypes = Array(
      "mysql", "hivesql", "kafka", "spark", "flink", "hivesql", "spark", "spark", "spark"
    )

    val random: Random = new Random()
    while (true) {
      val index: Int = random.nextInt(deviceTypes.length)
      val deviceId: String = s"deviceId_${(index + 1) * 10 + random.nextInt(index + 1)}"
      val deviceType: String = deviceTypes(index)
      val deviceSignal: Int = 10 + random.nextInt(90)
      // 模拟构造设备数据
      val deviceData = DeviceData(deviceId, deviceType, deviceSignal, System.currentTimeMillis())
      // 转换为JSON字符串
      val deviceJson: String = new Json(org.json4s.DefaultFormats).write(deviceData)
      println(deviceJson)
      Thread.sleep(100 + random.nextInt(500))

      val record = new ProducerRecord[String, String]("InternetOfThingsLog", deviceJson)
      producer.send(record)
    }

    // 关闭连接
    producer.close()
  }

  /**
   * 物联网设备发送状态数据 schema
   */
  case class DeviceData(
                         deviceId: String, //设备标识符ID
                         deviceType: String, //设备类型，如服务器mysql, redis, kafka或路由器route
                         signal: Double, //设备信号
                         time: Long //发送数据时间
                       )
}
