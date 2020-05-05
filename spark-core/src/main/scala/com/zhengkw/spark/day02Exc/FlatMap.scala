package com.zhengkw.spark.day02Exc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @ClassName:FlatMap
 * @author: zhengkw
 * @description:
 * @date: 20/05/05下午 11:28
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object FlatMap {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("FlatMap").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list1 = List(30, 50, 70, 60, 10, 20)
    val rdd1: RDD[Int] = sc.parallelize(list1, 2)
    val rdd2 = rdd1.flatMap(x => List(x * x))
    rdd2.collect().foreach(x => print(x + " "))
  }
}
