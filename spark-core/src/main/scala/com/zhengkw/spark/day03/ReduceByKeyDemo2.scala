package com.zhengkw.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:ReduceByKeyDemo2
 * @author: zhengkw
 * @description:
 * @date: 20/05/07下午 8:03
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object ReduceByKeyDemo2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ReduceByKey").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list1 = List("hello" -> 1, "hello" -> 5, "world" -> 7, "world" -> 1, "hello" -> 2)
    val rdd = sc.parallelize(list1, 2)
    val rdd2 = rdd.reduceByKey(_.max(_))
    rdd2.collect().foreach(println)

  }
}
