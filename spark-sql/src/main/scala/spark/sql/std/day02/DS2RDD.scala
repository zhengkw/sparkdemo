package spark.sql.std.day02

import org.apache.spark.sql.SparkSession

/**
 * @ClassName:DS2RDD
 * @author: zhengkw
 * @description:
 * @date: 20/05/14上午 9:34
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object DS2RDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("DS2RDD")
      .getOrCreate()
    //val df = spark.read.json("E:\\IdeaWorkspace\\sparkdemo\\data\\people.json")
    val list = User(21, "nokia") :: User(18, "java") :: User(20, "scala") :: User(20, "nova") :: Nil

    import spark.implicits._
    val rdd = spark.sparkContext.parallelize(list)
    val ds = rdd.toDS()
    ds.rdd.collect().foreach(println)
  }
}

case class User(age: Int, name: String)