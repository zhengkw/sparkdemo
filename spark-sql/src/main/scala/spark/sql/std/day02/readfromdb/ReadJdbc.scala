package spark.sql.std.day02.readfromdb

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
 * @ClassName:ReadJdbc
 * @author: zhengkw
 * @description:
 * @date: 20/05/14下午 3:43
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object ReadJdbc {
  def main(args: Array[String]): Unit = {
    //方法1
    /*  val url = "jdbc:mysql://hadoop102:3306/rdd"
      val props: Properties = new Properties()
      props.setProperty("user","root")
      props.setProperty("password","sa")

      val spark = SparkSession.builder()
        .master("local[2]")
        .appName("ReadJdbc ")
        .getOrCreate()
      import spark.implicits._

      spark.read.jdbc(url,"user",props).show()
      */
    //方法2
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("ReadJdbc ")
      .getOrCreate()
    import spark.implicits._
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val dfr = spark.read.format("jdbc")
      .option("user", "root")
      .option("password", "sa")
      .option("dbtable", "user")
      .option("url", url)
    val df = dfr.load()
    df.show()
  }
}
