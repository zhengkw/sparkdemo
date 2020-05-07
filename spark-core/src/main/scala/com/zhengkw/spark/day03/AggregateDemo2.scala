package com.zhengkw.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:AggregateDemo2
 * @author: zhengkw
 * @description:
 * @date: 20/05/07下午 9:39
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object AggregateDemo2 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf().setMaster("local[2]").setAppName("aggregate1"))
    val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 7)), 2)
    //分区内的最大最小值，分区外 最大值求和 最小值求和
    /* val tuple = rdd.aggregate((Int.MinValue, Int.MaxValue))(
       {
         case kv => (kv._1._1.max(kv._2._2), kv._1._2.min(kv._2._2))
       }
       , {
             //zero会参与运算！ 导致结果和需求不对
         case ((max1, min1), (max2, min2)) => (max1 + max2, min1 + min2)
       }
     )
     println(tuple.toString())*/
    val rdd2 = rdd.aggregateByKey((Int.MinValue, Int.MaxValue))(
      {
        case ((max, min), value) => (max.max(value), min.min(value))
      },
      {
        case ((max1, min1), (max2, min2)) => (max1 + max2, min1 + min2)
      }
    )
    rdd2.collect().foreach(println)
  }
}
