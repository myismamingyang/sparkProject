package spark.core.checkPoint

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/7 14:39
 * @Version: 1.0
 * @Function: 持久化
 */
object checkPoint {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val result: RDD[(String, Int)] = sc.textFile("data/input/words.txt")
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
    // 1 =注意:result-RDD计算成本较高,且后续会被频繁使用,所以可以放入缓存中,后面直接从缓存中获取=
    result.persist(StorageLevel.MEMORY_AND_DISK) //开发中一般设置内存+磁盘进行缓存

    // 2 =注意:为了保证数据的绝对安全,将rdd结果数据存入checkpoint中(实际中就是存在HDFS)=
    //后面的运行的时候会先从缓存中找,找到直接使用!没找到再去Checkpoint中找
    sc.setCheckpointDir("./ckp") //实际开发写HDFS路径
    result.checkpoint()

    result.foreach(println)

    println("============sortBy:适合大数据量排序====================")
    val sortedResult1: RDD[(String, Int)] = result.sortBy(_._2, false) //false表示逆序
    sortedResult1.take(3).foreach(println)
    println("============sortByKey:适合大数据量排序====================")
    val sortedResult2: RDD[(Int, String)] = result.map(_.swap).sortByKey(false) //swap表示交换位置
    sortedResult2.take(3).foreach(println)
    println("============top:适合小数据量排序====================")
    val sortedResult3: Array[(String, Int)] = result.top(3)(Ordering.by(_._2)) //注意:top本身就是取最大的前n个
    sortedResult3.foreach(println)

    //清空该缓存
    result.unpersist()

    sc.stop()
  }
}