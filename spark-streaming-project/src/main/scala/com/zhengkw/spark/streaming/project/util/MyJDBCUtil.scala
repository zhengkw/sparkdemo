package com.zhengkw.spark.streaming.project.util

import java.sql.{Connection, DriverManager}

/**
 * @ClassName:MyJDBCUtil
 * @author: zhengkw
 * @description:
 * @date: 20/05/19上午 10:31
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object MyJDBCUtil {
  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://hadoop102:3306/rdd"
  val user = "root"
  val pw = "sa"
  val conn: Connection = null
  Class.forName(driver)

  def getConn() = {
    if (conn != null) conn
    else DriverManager.getConnection(url, user, pw)
  }

  def closeConn() = {
    if (conn != null) conn.close()
  }
}
