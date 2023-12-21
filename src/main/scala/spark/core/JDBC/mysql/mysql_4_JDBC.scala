package spark.core.JDBC.mysql

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/7 23:25
 * @Version: 1.0
 * @Function: sparkSQL 4种查询 mysql方式
 */
object mysql_4_JDBC {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[4]").appName("4JDBC").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val ip = "node3"
    val db = "testLibrary"
    val user = "root"
    val password = "MMYqq123"

    val url = "jdbc:mysql://" + ip + ":3306/" + db
    val props = new Properties()
    props.put("driver", "com.mysql.cj.jdbc.Driver")
    props.put("user", user)
    props.put("password", password)
    props.put("url", url)
    props.put("fetchSize", "100")

    //read1(spark, url, "table1", props).show()

    //read2(spark, "table2", "id", url, 1, 10000, 10, props).show()

    ///read3(spark, "table3", "id", url, 1, 10000, 10, props).show()

    //read4(spark, url, "select * from xxx where yyy = 1 limit 100").show()

    spark.close()
  }

  /**
   * 单分区读，且是全量读，应用：表数据量小的本地测试
   */
  def read1(spark: SparkSession, url: String, table: String, props: Properties): DataFrame = {
    spark.read.jdbc(url, table, props)
  }

  /**
   * 多分区读，但条件列必须是数值列，且limit无法下推mysql，即使给upperBound = 10 也会全量读
   */
  def read2(spark: SparkSession, table: String, column: String, url: String, lower: Long, upper: Long,
            parts: Int, props: Properties): DataFrame = {
    spark.read.jdbc(url, table, column, lower, upper, parts, props)
  }

  /**
   * 多分区读，任意条件，limit可下推mysql
   */
  def read3(spark: SparkSession, table: String, column: String, url: String, lower: Long, upper: Long, parts: Int,
            props: Properties): DataFrame = {
    val step = ((upper - lower) / parts).toInt
    val predicates: Array[String] = 1.to(parts).map(index => {
      val lowerBound = (index - 1) * step + lower
      val upperBound = index * step + lower
      column + " >=" + lowerBound + " and " + column + " < " + upperBound
    }).toArray
    predicates.foreach(println)
    spark.read.jdbc(url, table, predicates, props)
  }

  /**
   * 单分区读，可以limit，应用：不管表大小，只抽取前n条
   */
  def read4(spark: SparkSession, url: String, sql: String): DataFrame = {
    spark.read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "xx")
      .option("password", "xx")
      .option("url", url)
      .option("dbtable", s"($sql) dbtable").load()
  }
}
