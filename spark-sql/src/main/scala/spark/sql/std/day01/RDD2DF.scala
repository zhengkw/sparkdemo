package spark.sql.std.day01

import org.apache.spark.sql.SparkSession

/**
 * @ClassName:RDD2DF
 * @author: zhengkw
 * @description:
 * @date: 20/05/13上午 11:10
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object RDD2DF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("RDD2DF")
      .getOrCreate()
    //通过session创建一个sparkcontext
    val sc = spark.sparkContext
    val rdd = sc.parallelize(List((10, "a"), (20, "b"), (30, "c"), (40, "d")))
    //导入SparkSession类中的implicits对象里的所有方法
    import spark.implicits._
    val df = rdd.toDF("age", "name")

    df.printSchema()
    val view = df.createOrReplaceTempView("user")
    //只显示top20
    val df2 = spark.sql("select * from user")
    df2.show()
    //调用的都一样
    df.show()

  }
}
