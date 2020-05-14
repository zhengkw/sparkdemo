package spark.sql.std.day02

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @ClassName:RDD2DF
 * @author: zhengkw
 * @description:
 * @date: 20/05/14上午 10:48
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object RDD2DF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("RDD2DF_2")
      .getOrCreate()
    //通过session创建一个sparkcontext
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Array((10, "lisi"), (20, "zs"), (15, "ww")))
      .map {
        case (age, name) => Row(age, name)
      }
    val schema=StructType(Array(StructField("age",IntegerType),StructField("name",StringType)))
    val df = spark.createDataFrame(rdd, schema)
    df.show()

  }
}
