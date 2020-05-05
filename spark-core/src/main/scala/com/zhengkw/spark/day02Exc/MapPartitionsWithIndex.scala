package com.zhengkw.spark.day02Exc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @ClassName:MapPartitionsWithIndex
 * @author: zhengkw
 * @description:
 * @date: 20/05/05下午 10:47
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object MapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MapPartitions").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list1 = List(30, 50, 70, 60, 10, 20) // 两个分区
    val rdd1: RDD[Int] = sc.parallelize(list1, 4)
    val rdd2 = rdd1.mapPartitionsWithIndex((index, it) => it.map(x => (x, index)))
    rdd2.collect().foreach(println)
  }
}
