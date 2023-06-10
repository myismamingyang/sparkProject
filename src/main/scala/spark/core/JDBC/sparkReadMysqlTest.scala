package spark.core.JDBC

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties
import java.util.Map

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/7 22:20
 * @Version: 1.0
 * @Function: spark 查询 mysql 数据
 */
object sparkReadMysqlTest {
  def main(args: Array[String]): Unit = {

    // Spark2.0始，spark使用SparkSession接口代替SQLcontext和HiveContext
    val spark = SparkSession.builder().appName("MysqlQueryDemo").master("local").getOrCreate()
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://node3:3306/testLibrary?useUnicode=true&characterEncoding=utf-8")
      .option("driver", "com.mysql.jdbc.Driver") //驱动
      .option("user", "root")
      .option("password", "MMYqq123")
      //.option("dbtable", s"(select COUNT(1) from customer) customer") 直接查询sql数据
      .option("dbtable", "customer") //加载表  // dbtable: 指定查询功能
      // s"(sql) dbtable"  为查询sql语法
      .load()
    jdbcDF.show(5)

    //注册成临时表
    println("spark SQL 注册成临时表")
    jdbcDF.createOrReplaceTempView("people")
    spark.sql("select COUNT(1) from people").show()
    // jdbcDF.createOrReplaceTempView 注册为临时表后sparkSQL可直接查询临时表
    println("spark SQL")
    spark.sql("select create_date_time from people").show()

  }
}
