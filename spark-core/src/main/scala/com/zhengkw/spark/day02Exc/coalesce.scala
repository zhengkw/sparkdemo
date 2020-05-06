package com.zhengkw.spark.day02Exc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:coalesce
 * @author: zhengkw
 * @description:
 * @date: 20/05/06下午 10:54
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object coalesce {
  def main(args: Array[String]): Unit = {
    val list1 = List(30, 50, 70, 60, 10, 20)
    val sc = new SparkContext(
      new SparkConf().setMaster("local[2]").setAppName("coalesce"))
    val rdd1 = sc.parallelize(list1, 2)
    val rdd2 = rdd1.coalesce(1)
    println(rdd2.getNumPartitions)
    val rdd3 = rdd1.repartition(3)
    println(rdd3.getNumPartitions)
  }
}
