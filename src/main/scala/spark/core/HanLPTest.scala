package spark.core

import java.util

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/7 14:39
 * @Version: 1.0
 * @Function: HanLP 分词器
 */
object HanLPTest {
  def main(args: Array[String]): Unit = {
    val words = "[HanLP入门案例]"
    println("1: " + words)
    val terms: util.List[Term] = HanLP.segment(words)
    println("2: " + terms) //直接打印java的list:[[/w, HanLP/nx, 入门/vn, 案例/n, ]/w]
    import scala.collection.JavaConverters._
    println("3: " + terms.asScala.map(_.word)) //转为scala的list:ArrayBuffer([, HanLP, 入门, 案例, ])
    val cleanWords1: String = words.replaceAll("\\[|\\]", "") //将"["或"]"替换为空"" //"HanLP入门案例"
    //      "\\[|\\]"     \\(编译)[   |(或者)  \\(编译)]
    println("4: " + cleanWords1) //HanLP入门案例
    println("5: " + HanLP.segment(cleanWords1).asScala.map(_.word)) //ArrayBuffer(HanLP, 入门, 案例)

    val log = """00:00:00 2982199073774412    [360安全卫士]   8 3 download.it.com.cn/softweb/software/firewall/antivirus/20067/17938.html"""
    val cleanWords2 = log.split("\\s+")(2) //[360安全卫士]
      .replaceAll("\\[|\\]", "") //360安全卫士
    println("6: " + HanLP.segment(cleanWords2).asScala.map(_.word)) //ArrayBuffer(360, 安全卫士)
    val str: String = log.split(" +")(5)
    println(str)
  }
}