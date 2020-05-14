package spark.sql.std.day02.readfromdb

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
 * @ClassName:WriteJDBC
 * @author: zhengkw
 * @description:
 * @date: 20/05/14下午 4:11
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object WriteJDBC {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("WriteJDBC ")
      .getOrCreate()
    import spark.implicits._
    val df = spark.read.json("E:\\IdeaWorkspace\\sparkdemo\\data\\people.json")
    //方式1
    /* df.write.format("jdbc")
       .options(Map(
         "url" ->"jdbc:mysql://hadoop102:3306/rdd",
         "password" -> "sa",
         "user"->"root",
         "dbtable" ->"zhengkw"
       )).save()*/
    val props = new Properties
    props.setProperty("user", "root")
    props.setProperty("password", "sa")
    //方式2
    df.write.mode("overwrite").jdbc("jdbc:mysql://hadoop102:3306/rdd", "zhengkw",props)
  }
}
