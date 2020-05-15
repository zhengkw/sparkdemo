package spark.sql.std.day03

import org.apache.spark.sql.SparkSession

/**
 * @ClassName:SparkSQL
 * @author: zhengkw
 * @description:
 * @date: 20/05/15下午 1:45
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object SparkSQL {
  def main(args: Array[String]): Unit = {
    //不加会出现权限问题！
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("SparkSQL")
      .config("spark.sql.shuffle.partitions", 10)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    spark.sql("use spark")
    // val df = spark.read.json("file:///E:\\IdeaWorkspace\\sparkdemo\\data\\people.json")
    // val df = spark.read.json("/input/people.json")
    // df.write.saveAsTable("people_")
    //  val df = List((11L, "a"), (22L, "b")).toDF("n", "a")
   val df = List(("adc", 11L), ("bbbc", 22L)).toDF("name", "age")
    df.write.mode("append").saveAsTable("people")
    spark.sql("select * from people").show

  }
}
