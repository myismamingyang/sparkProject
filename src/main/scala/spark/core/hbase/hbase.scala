package spark.core.hbase

import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.hbase.client.Put
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
//import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/12/22 22:33
 * @Version: 1.0
 * @Function:
 */
object hbase {
  def main(args: Array[String]): Unit = {
//    val sparkConf: SparkConf = new SparkConf()
//      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
//      .setMaster("local[*]")
//    val sc: SparkContext = new SparkContext(sparkConf)
//    sc.setLogLevel("WARN")
//
//    // 构建RDD
//    val list = List(("hadoop", 234), ("spark", 3454), ("hive", 343434), ("ml", 8765))
//    val outputRDD: RDD[(String, Int)] = sc.parallelize(list, numSlices = 2)
//
//    // 将数据写入到HBase表中, 使用saveAsNewAPIHadoopFile函数，要求RDD是(key, Value)
//    //  组装RDD[(ImmutableBytesWritable, Put)]
//    /**
//     * HBase表的设计：
//     * 表的名称：htb_wordcount
//     * Rowkey:  word
//     * 列簇:    info
//     * 字段名称： count
//     */
//    val putsRDD: RDD[(ImmutableBytesWritable, Put)] = outputRDD.mapPartitions { iter =>
//      iter.map { case (word, count) =>
//        // 创建Put实例对象
//        val put = new Put(Bytes.toBytes(word))
//        // 添加列
//        put.addColumn(
//          // 实际项目中使用HBase时，插入数据，先将所有字段的值转为String，再使用Bytes转换为字节数组
//          Bytes.toBytes("info"), Bytes.toBytes("cout"), Bytes.toBytes(count.toString)
//        )
//        // 返回二元组
//        (new ImmutableBytesWritable(put.getRow), put)
//      }
//    }
//
//    // 构建HBase Client配置信息
//    val conf: Configuration = HBaseConfiguration.create()
//    // 设置连接Zookeeper属性
//    conf.set("hbase.zookeeper.quorum", "node1")
//    conf.set("hbase.zookeeper.property.clientPort", "2181")
//    conf.set("zookeeper.znode.parent", "/hbase")
//    // 设置将数据保存的HBase表的名称
//    conf.set(TableOutputFormat.OUTPUT_TABLE, "htb_wordcount")
//    /*
//         def saveAsNewAPIHadoopFile(
//             path: String,// 保存的路径
//             keyClass: Class[_], // Key类型
//             valueClass: Class[_], // Value类型
//             outputFormatClass: Class[_ <: NewOutputFormat[_, _]], // 输出格式OutputFormat实现
//             conf: Configuration = self.context.hadoopConfiguration // 配置信息
//         ): Unit
//     */
//    putsRDD.saveAsNewAPIHadoopFile(
//      "datas/spark/htb-output-" + System.nanoTime(), //
//      classOf[ImmutableBytesWritable], //
//      classOf[Put], //
//      classOf[TableOutputFormat[ImmutableBytesWritable]], //
//      conf
//    )
//
//    // 应用程序运行结束，关闭资源
//    sc.stop()
//  }
}
