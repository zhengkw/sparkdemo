package com.zhengkw.spark.day02Exc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:MapPartitions
 * @author: zhengkw
 * @description:
 * @date: 20/05/05下午 10:02
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object MapPartitions {
  def main(args: Array[String]): Unit = {
    val list1 = List(30, 5, 7, 60, 10, 20)
    val sc = new SparkContext(new SparkConf().setAppName("mapPartitions").setMaster("local[2]"))
    val rdd1 = sc.parallelize(list1, 2)
    val rdd2 = rdd1.mapPartitions(it => it.map(x => x * 2))
     rdd2.collect().foreach(println)
  }
}
