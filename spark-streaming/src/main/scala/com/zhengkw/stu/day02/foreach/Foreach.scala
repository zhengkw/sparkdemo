package com.zhengkw.stu.day02.foreach

import java.net.Socket

import com.zhengkw.stu.day01.RDDQueue
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName:Foreach
 * @author: zhengkw
 * @description: 从socket获取数据 存到mysql
 * @date: 20/05/18下午 2:28
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object Foreach {
  def main(args: Array[String]): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val user = "root"
    val pw = "sa"

    val conf = new SparkConf().setMaster("local[*]").setAppName("Foreach")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("./ck2")
    //获取ss
    val ss = SparkSession.builder()
      .config(ssc.sparkContext.getConf)
      .getOrCreate()

    //从socket获取数据
    val linestream = ssc.socketTextStream("hadoop102", 9876)
    val result = linestream.flatMap(_.split(" "))
      .map((_, 1))
      .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => {
        Some(seq.sum + opt.getOrElse(0))
      })
    //通过sparksession获得df来操作sql
    import ss.implicits._
    result.foreachRDD(rdd => {
      rdd.toDF("word", "count")
        .write.mode("overwrite")
        .format("jdbc")
        .option("url", url)
        .option("user", user)
        .option("password", pw)
        .option("dbtable", "word")
        .save()

    })
    ssc.start()
    ssc.awaitTermination()
  }
}
