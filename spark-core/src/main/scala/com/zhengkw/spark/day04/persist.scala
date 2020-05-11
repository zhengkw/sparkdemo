package com.zhengkw.spark.day04

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:persist
 * @author: zhengkw
 * @description:
 * @date: 20/05/08下午 3:24
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object persist {
  def main(args: Array[String]): Unit = {

    val list = List("hello world", "hello")
    val sc = new SparkContext(new SparkConf()
      .setMaster("local[2]").setAppName("persist"))
    val rdd = sc.parallelize(list, 2)
    val rdd2 = rdd.flatMap(x => {
      println("flatmap..")
      x.split(" ")
    })
      .map(x => {
        println("map....")
        (x, 1)
      })
    rdd2.persist()
    rdd2.collect
    // rdd2.cache()

    println("--------------")
    rdd2.collect
    println("--------------")
    rdd2.collect
    Thread.sleep(10000000)
  }
}
