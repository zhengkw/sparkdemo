package spark.sql.std.day02.exc

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}

/**
 * @ClassName:UDAF_Sum
 * @author: zhengkw
 * @description:
 * @date: 20/05/14下午 10:57
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object UDAF_Sum {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("UDAF_Sum ")
      .getOrCreate()
    val df = spark.read.json("E:\\IdeaWorkspace\\sparkdemo\\data\\people.json")
    df.createOrReplaceTempView("p")
    //区别于注册累加器 sc.register
    spark.udf.register("mySum", new mySum)
    spark.sql("select mySum(age) _age from p").show()
  }
}

class mySum extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(StructField("age", LongType) :: Nil)

  override def bufferSchema: StructType = StructType(StructField("sum", LongType) :: Nil)

  override def dataType: DataType = LongType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit =
    buffer(0) = 0L

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
  }

  override def evaluate(buffer: Row) = buffer.getLong(0)
}