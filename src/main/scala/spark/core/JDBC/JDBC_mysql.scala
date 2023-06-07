package spark.core.JDBC

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}
/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/7 15:35
 * @Version: 1.0
 * @Function:  jdbc demo
 */
object JDBC_mysql {
  def main(args: Array[String]): Unit = {
    //1.准备环境(Env)sc-->SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //2.加载数据
    //RDD[(姓名, 年龄)]
    val data: RDD[(String, Int)] = sc.parallelize(List(("jack", 18), ("tom", 19), ("rose", 20)))

    //3.将数据保存到MySQL
    /*data.foreach(row=>{
      //开启连接-多少条数据就要开启关闭多少次
      //关闭连接-
    })*/

    data.foreachPartition(rows=>{
      //开启连接-多少个区就开启关闭多少次
      val conn: Connection = DriverManager.getConnection("jdbc:mysql://node3:3306/bigdata?characterEncoding=UTF-8","root","MMYqq188")
      val sql:String = "INSERT INTO `t_student` (`id`, `name`, `age`) VALUES (NULL, ?, ?);"
      val ps: PreparedStatement = conn.prepareStatement(sql)
      rows.foreach(row=>{
        val name: String = row._1
        val age: Int = row._2
        ps.setString(1,name)
        ps.setInt(2,age)
        ps.addBatch()//将该条数据添加到批处理中
      })
      ps.executeBatch()//执行批处理sql

      //关闭连接-
      if (conn != null) conn.close()
      if (ps != null) ps.close()
    })

    //4.从MySQL读取数据
    /*
    class JdbcRDD[T: ClassTag](
      sc: SparkContext,
      getConnection: () => Connection,
      sql: String,
      lowerBound: Long,
      upperBound: Long,
      numPartitions: Int,
      mapRow: (ResultSet) => T = JdbcRDD.resultSetToObjectArray _
      )
     */
    val getConnection =  () =>DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8","root","root")
    val querySQL = "select id,name,age from t_student where ? <= id and id <= ?"
    val mapRow = (rs:ResultSet) =>{
      val id: Int = rs.getInt("id")
      val name: String = rs.getString("name")
      val age: Int = rs.getInt("age")
      (id,name,age)
    }
    val studentRDD = new JdbcRDD[(Int, String, Int)](
      sc,
      getConnection,
      querySQL,
      4, //下界
      6, //上界
      2, //用几个分区去读取
      mapRow //结果集处理函数
    )
    studentRDD.foreach(println)

    sc.stop()
  }
}
