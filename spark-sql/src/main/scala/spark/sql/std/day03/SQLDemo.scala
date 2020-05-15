package spark.sql.std.day03

import java.text.DecimalFormat

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.immutable.Nil

/**
 * @ClassName:SQLDemo
 * @author: zhengkw
 * @description:
 * @date: 20/05/15下午 9:11
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object SQLDemo {
  def main(args: Array[String]): Unit = {
    //不加会出现权限问题！
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("SQLDemo")
      .config("spark.sql.shuffle.partitions", 10)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    spark.sql("use spark")
    spark.udf.register("remark", new CityRemarkUDAF)
    // 1. 先把需要的字段, join, 查出来  t1
    spark.sql(
      """
        |select
        |    ci.city_name,
        |    ci.area,
        |    pi.product_name,
        |    uva.click_product_id
        |from user_visit_action uva
        |join product_info pi on uva.click_product_id=pi.product_id
        |join city_info ci on uva.city_id=ci.city_id
        |""".stripMargin).createOrReplaceTempView("t1")

    // 2. 按照地区商品分组, 聚合  t2
    spark.sql(
      """
        |select
        |    area,
        |    product_name,
        |    count(*) count,
        |    remark(city_name) remark
        |from t1
        |group by area, product_name
        |""".stripMargin).createOrReplaceTempView("t2")

    // 3. 开窗, 排序(降序) t3   // rank(1 2 2 4 5)   row_number(1 2 3 4 5...)   dense_rank(1 2 2 3 4)

    spark.sql(
      """
        |select
        |    area,
        |    product_name,
        |    count,
        |    remark,
        |    ROW_NUMBER () over(partition by area order by count desc) rk
        |from t2
        |""".stripMargin).createOrReplaceTempView("t3")

    // 4. top3
    spark
      .sql(
        """
          |select
          |    area,
          |    product_name,
          |    count,
          |    remark
          |from t3
          |where rk<=3
          |""".stripMargin)
      .coalesce(1) // 降低分区
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("result")
    //.show(1000, false) // 参数2: 是否截断. 如果内容太长, 默认截断 ...

    spark.sql("select * from result").show(1000, false)
    spark.close()

  }
}

class CityRemarkUDAF extends UserDefinedAggregateFunction {
  //输入一个城市名称
  override def inputSchema: StructType = StructType(StructField("cityName", StringType) :: Nil)

  // 缓冲一个 map (city->count) ,totalcount
  override def bufferSchema: StructType = StructType(StructField("citycount", MapType(StringType, LongType)) :: StructField("total", LongType) :: Nil)

  //输出一个字符串  北京21.2%，天津13.2%，其他65.6%   排序取前2
  override def dataType: DataType = StringType

  //是否没有有副作用
  override def deterministic: Boolean = true

  //初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String, Long]()
    buffer(1) = 0L
  }

  //分区内合并
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    /*input match {
      //input是个row 放了个string
      case Row(cityName: String) =>
        //total
        buffer(1) = buffer.getLong(1) + 1L
        // 更新具体城市的数量
        val map = buffer.getMap[String, Long](0)
        buffer(0) = map + (cityName -> (map.getOrElse(cityName, 0L) + 1L))
      case _ =>
    }*/
    input match {
      // 传入了一个城市
      case Row(cityName: String) =>
        // 北京
        // 1. 先更新总数
        buffer(1) = buffer.getLong(1) + 1L
        // 2. 再去更新具体城市的数量
        val map = buffer.getMap[String, Long](0)
        buffer(0) = map + (cityName -> (map.getOrElse(cityName, 0L) + 1L))
      // 传入了一个null
      case _ =>
    }


  }

  //分区间合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    /*  buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)*/
    // 把数据合并, 再放入到buffer1中
    /*  val map1 = buffer1.getMap[String, Long](0)
      val total1 = buffer1.getLong(1)

      val map2 = buffer2.getMap[String, Long](0)
      val total2 = buffer2.getLong(1)

      // 1. 先合并总数
      buffer1(1) = total1 + total2
      //聚合map
      map1.foldLeft(map2)({
        case (map, (city, count)) =>
          // 聚合map
          map + (city -> (map.getOrElse(city, 0L)+ count))
      })*/
    val map1 = buffer1.getMap[String, Long](0)
    val total1 = buffer1.getLong(1)

    val map2 = buffer2.getMap[String, Long](0)
    val total2 = buffer2.getLong(1)

    // 1. 先合并总数
    buffer1(1) = total1 + total2
    // 2. 合并map
    buffer1(0) = map1.foldLeft(map2) {
      case (map, (city, count)) =>
        map + (city -> (map.getOrElse(city, 0L) + count))
    }

  }

  // 北京21.2%，天津13.2%，其他65.6%   排序取前2
  override def evaluate(buffer: Row): Any = {
    val cityCount = buffer.getMap[String, Long](0)
    val total = buffer.getLong(1)

    // 北京21.2%，天津13.2%，其他65.6%   排序取前2
    val cityCountTop2: List[(String, Long)] = cityCount.toList.sortBy(-_._2).take(2)

    val cityRemarkTop2: List[CityRemark] = cityCountTop2.map {
      case (city, count) => CityRemark(city, count.toDouble / total)
    }
    // top2 + 其他
    val cityRemarks = cityRemarkTop2 :+ CityRemark("其他", cityRemarkTop2.foldLeft(1D)(_ - _.rate))

    cityRemarks.mkString(", ")
  }
}

case class CityRemark(city: String, rate: Double) {
  val f = new DecimalFormat(".00%")

  // 北京21.20%
  override def toString: String = s"$city:${f.format(math.abs(rate))}"
}