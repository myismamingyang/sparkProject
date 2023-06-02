package spark.core.transForMation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val value: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
    val function: (Int, Iterator[Int]) => Iterator[String] = (index: Int, iter: Iterator[Int]) => {
      iter.map(x => "partitionID-data: [" + index + "] " + x)
    }
    value.mapPartitionsWithIndex(function).foreach(println)
  }
}
