package com.zhengkw.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:ReduceByKey
 * @author: zhengkw
 * @description: 只能用于KV形式的聚合
 * @date: 20/05/07下午 5:13
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object ReduceByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ReduceByKey").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list1 = List("hello" -> 1, "hello" -> 2, "world" -> 2, "world" -> 1, "hello" -> 2)
    val rdd1 = sc.parallelize(list1, 2)
    rdd1.reduceByKey(_ + _).collect().foreach(println)
  }
}
