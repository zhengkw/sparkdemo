package spark.sql.std.day01

import org.apache.spark.sql.SparkSession

/**
 * @ClassName:RDD2DF_3
 * @author: zhengkw
 * @description: 集合转换成DF
 * @date: 20/05/22下午 5:03
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object RDD2DF_3 {
  def main(args: Array[String]): Unit = {
    //创建一个builder，从builder或者取session
    val spark = SparkSession.builder()
      .appName("DF2RDD_3")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    //集合
    val map = Map("a" -> 3, "b" -> 4, "c" -> 5)
    val df = map.toList.toDF("x", "y")
    df.show()
    df.printSchema()
    spark.close()
  }
}
