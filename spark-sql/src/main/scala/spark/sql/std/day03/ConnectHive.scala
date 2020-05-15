package spark.sql.std.day03

import org.apache.spark.sql.SparkSession

/**
 * @ClassName:ConnectHive
 * @author: zhengkw
 * @description:
 * @date: 20/05/15上午 10:55
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object ConnectHive {
  def main(args: Array[String]): Unit = {
    //不加会出现权限问题！
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("ConnectHive")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    // spark.sql("show databases")
    //spark.sql("select * from gmall.dwt_uv_topic").show()
    //spark.sql("select * from gmall.ods_user_info").show()
    spark.close()
  }
}
