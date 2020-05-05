package com.zhengkw.spark.day02Exc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:CreateRDD
 * @author: zhengkw
 * @description:
 * @date: 20/05/05下午 9:33
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object CreateRDD {
  def main(args: Array[String]): Unit = {
    val list1 = List(30, 50, 70, 60, 10, 20)
    val sc = new SparkContext(
      new SparkConf().setMaster("local[2]").setAppName("rdd"))
    val rdd1 = sc.parallelize(list1, 2)
    //打印rdd 先将rdd转数组
    rdd1.collect.foreach(println)
  }
}
