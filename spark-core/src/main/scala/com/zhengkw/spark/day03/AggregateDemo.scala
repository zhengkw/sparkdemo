package com.zhengkw.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:AggregateDemo
 * @author: zhengkw
 * @description:
 * @date: 20/05/06上午 10:59
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object AggregateDemo {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf().setMaster("local[2]").setAppName("aggregate"))
    val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
    //rdd.glom().map(_.toList).collect().foreach(println)
    //每个分区取最大值，分区外求和
    val rdd2 = rdd.aggregateByKey(Int.MinValue)(
      (u, v) => u.max(v),
      (u, m) => u + m
    )
    rdd2.collect().foreach(print)
  }
}