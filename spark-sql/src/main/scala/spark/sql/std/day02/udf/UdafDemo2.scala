package spark.sql.std.day02.udf

import java.text.DecimalFormat

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StringType, StructField, StructType}

/**
 * @ClassName:UdafDemo2
 * @author: zhengkw
 * @description:
 * @date: 20/05/14下午 2:12
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object UdafDemo2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("DSDemo1")
      .getOrCreate()
    import spark.implicits._
    val df = spark.read.json("E:\\IdeaWorkspace\\sparkdemo\\data\\people.json")
    //注册udf函数
    spark.udf.register("myAvg", new myAvg)
    df.createOrReplaceTempView("p")
    spark.sql("select myAvg(age) avg_age from p").show()
  }

}

class myAvg extends UserDefinedAggregateFunction {
  override def inputSchema: StructType =
    StructType(StructField("ele", DoubleType) :: Nil)

  /**
   * @descrption: 求平均值需要一个数据的和  一个数据有效个数
   * @return: org.apache.spark.sql.types.StructType
   * @date: 20/05/14 下午 2:29
   * @author: zhengkw
   */
  override def bufferSchema: StructType =
    StructType(StructField("sum", DoubleType) :: StructField("count", LongType) :: Nil)

  //返回值类型 最终聚合结果的类型 （返回一个格式化的数据 工具类返回的是string）
  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0D
    buffer(1) = 0L
  }

  /**
  * @descrption: buffer(0)  -> sum  buffer(1) -> count
  * @return: void
  * @date: 20/05/14 下午 2:30
  * @author: zhengkw
  */
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

  override def evaluate(buffer: Row): Any = {
    new DecimalFormat(".00").format(buffer.getDouble(0) / buffer.getLong(1))
  }
}