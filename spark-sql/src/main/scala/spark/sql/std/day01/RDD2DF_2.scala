package spark.sql.std.day01

import org.apache.spark.sql.SparkSession

import scala.collection.mutable


/**
 * @ClassName:RDD2DF_2
 * @author: zhengkw
 * @description:
 * @date: 20/05/13上午 11:35
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object RDD2DF_2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RDD2DF_2")
      .master("local[2]")
      .getOrCreate()
    val list = List(User(22, "java"), User(23, "keke"))
    val list1 = list :+ User(15, "ww")
    // list.foreach(println)

    val rdd = spark.sparkContext.parallelize(list1)
    import spark.implicits._
    val df = rdd.toDF("age", "name")
    df.show()

  }
}

case class User(age: Int, name: String)