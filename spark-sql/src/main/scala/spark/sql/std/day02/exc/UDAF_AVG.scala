package spark.sql.std.day02.exc

import java.text.DecimalFormat

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

/**
 * @ClassName:UDAF_AVG
 * @author: zhengkw
 * @description:
 * @date: 20/05/14下午 11:31
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object UDAF_AVG {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("UDAF_AVG")
      .getOrCreate()
    val df = spark.read.json("E:\\IdeaWorkspace\\sparkdemo\\data\\people.json")
    df.createOrReplaceTempView("p")
    //区别于注册累加器 sc.register
    spark.udf.register("myAvg", new myAvg)
    spark.sql("select myAvg(age) _age from p").show()
  }
}

class myAvg extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(Array(StructField("age", DoubleType)))

  override def bufferSchema: StructType = StructType(Array(StructField("sum", DoubleType), StructField("count", LongType)))

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0D
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getDouble(0) + input.getDouble(0)
      buffer(1) = buffer.getLong(1) + 1L
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)

  }

  override def evaluate(buffer: Row) =
    new DecimalFormat(".00").format(buffer.getDouble(0) / buffer.getLong(1)).toDouble
}