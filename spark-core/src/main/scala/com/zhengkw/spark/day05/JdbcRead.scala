package com.zhengkw.spark.day05

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:JdbcRead
 * @author: zhengkw
 * @description:
 * @date: 20/05/09上午 11:40
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object JdbcRead {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("jdbc"))
    val driver = "com.mysql.jdbc.Driver"
    val user = "root"
    val pwd = "sa"
    val url = "jdbc:mysql://hadoop102:3306:rdd"
    var sql = ""
    val rdd = new JdbcRDD(
      sc,
      () => {
        Class.forName(driver)
        val conn = DriverManager.getConnection(url, user, pwd)
        conn
      },
      sql,
      1,
      3,
      2
    )
  }
}
