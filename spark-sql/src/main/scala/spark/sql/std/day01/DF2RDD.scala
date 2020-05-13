package spark.sql.std.day01

import org.apache.spark.sql.SparkSession

/**
 * @ClassName:DF2RDD
 * @author: zhengkw
 * @description:
 * @date: 20/05/13上午 10:52
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object DF2RDD {
  def main(args: Array[String]): Unit = {
    //创建一个builder，从builder或者取session
    val spark = SparkSession.builder()
      .appName("DF2RDD")
      .master("local[2]")
      .getOrCreate()
    //获得df
    val df = spark.read.json("E:\\IdeaWorkspace\\sparkdemo\\data\\people.json")
    df.printSchema()
    val rdd = df.rdd
    val result = rdd.map(row => {
      val age = row.get(0)
      //row.getAs()
     // row.getLong()
      val name = row.get(1)
      (age, name)
    }
    )
    result.collect.foreach(println)

  }
}
