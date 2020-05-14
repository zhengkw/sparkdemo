package spark.sql.std.day02

import org.apache.spark.sql.SparkSession

/**
 * @ClassName:DSDemo1
 * @author: zhengkw
 * @description:
 * @date: 20/05/14上午 9:14
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object DSDemo1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("DSDemo1")
      .getOrCreate()
    val list = List(10, 23, 40, 15, 18, 19, 50, 70)
    import spark.implicits._
    val ds = list.toDS()
    ds.show()
    ds.filter(_ > 30).show()
    ds.createOrReplaceTempView("demo")
    spark.sql("select * from demo where value >40").show()
    val list1= User(21,"kava")::User(18,"nava")::User(20,"java")::User(20,"java")::Nil
    list1.toDS().show()
  }
}

case class User(age: Int, name: String)