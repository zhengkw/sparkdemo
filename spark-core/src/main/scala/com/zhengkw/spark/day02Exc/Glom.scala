package com.zhengkw.spark.day02Exc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @ClassName:Glom
 * @author: zhengkw
 * @description:
 * @date: 20/05/05下午 11:40
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object Glom {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Glom").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list1 = List(30, 50, 70, 60, 10, 20)
    val rdd1: RDD[Int] = sc.parallelize(list1, 3)
    val rdd2 = rdd1.glom()
       rdd2.collect().map(_.toList).foreach(println)
  }
}
