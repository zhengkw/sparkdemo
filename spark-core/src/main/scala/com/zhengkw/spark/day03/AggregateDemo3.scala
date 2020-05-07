package com.zhengkw.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:AggregateDemo3
 * @author: zhengkw
 * @description:
 * @date: 20/05/07下午 10:19
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object AggregateDemo3 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf().setMaster("local[2]").setAppName("aggregate1"))
    val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 5)), 2)
    // 计算每个key的平均值!
    // 分区内: 求和 与key出现的个数  分区间: 和相加 与 个数相加
    val rdd2 = rdd.aggregateByKey((0, 0))(
      {
        case ((zero, count), value) => (zero + value, count + 1)
      }, {
        case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2)
      }
    )
    rdd2.map({
      case (k, (sum, count)) => (k, sum.toDouble / count)
    }).collect().foreach(println)
  }
}
