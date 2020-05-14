package spark.sql.std.day02.udf

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

/**
 * @ClassName:UdfDemo1
 * @author: zhengkw
 * @description:
 * @date: 20/05/14下午 1:51
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object UdafDemo1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("DSDemo1")
      .getOrCreate()
    import spark.implicits._
    val df = spark.read.json("E:\\IdeaWorkspace\\sparkdemo\\data\\people.json")
    //注册udf函数
    spark.udf.register("mySum", new mySum)
    df.createOrReplaceTempView("p")
    spark.sql("select mySum(age) _age from p").show()
  }
}

class mySum extends UserDefinedAggregateFunction {
  // 输入的数据的类型  StructType可以封装多个  Double
  // override def inputSchema: StructType = StructType(StructField("ele", DoubleType) :: Nil)
  override def inputSchema: StructType = StructType(Array(StructField("ele", LongType)))

  // 缓冲区的数据类型  LongType
  override def bufferSchema: StructType = StructType(Array(StructField("sum", LongType)))

  // 最终聚合结果的类型
  override def dataType: DataType = LongType

  override def deterministic: Boolean = true

  // 初始化: 对缓冲区输出化
  override def initialize(buffer: MutableAggregationBuffer): Unit =
  // 缓冲区, 可以看成一个集合, 当前只缓冲一个值  buffer(0)
    buffer(0) = 0L

  // 分区内聚合 参数1: 缓冲区 参数: 传过来的每行数据 行的列数由传给聚合函数的参数来定
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit =
  //判断传入数据是否有null 进行处理 否则报空指针
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
    }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit =
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)

  override def evaluate(buffer: Row): Any =
    buffer.getLong(0)
}