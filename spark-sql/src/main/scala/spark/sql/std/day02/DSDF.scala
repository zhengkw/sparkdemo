package spark.sql.std.day02

import org.apache.spark.sql.SparkSession

/**
 * @ClassName:DSDF
 * @author: zhengkw
 * @description:
 * @date: 20/05/14上午 10:06
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object DSDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("DSDF")
      .getOrCreate()
    import spark.implicits._
    val df = spark.read.json("E:\\IdeaWorkspace\\sparkdemo\\data\\people.json")
    val ds = df.as[People]
    ds.show()
    val df1 = ds.toDF()
    df1.show()

  }
}

case class People(age: Long, name: String)