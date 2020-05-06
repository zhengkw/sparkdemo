package com.zhengkw.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
 * @ClassName:WordCount
 * @author: zhengkw
 * @description:
 * @date: 20/05/06下午 2:25
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    var path = "F:\\mrinput\\wordcount\\test.txt"
    val conf: SparkConf = new SparkConf().setAppName("wordcount").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd = sc.textFile(path)
    rdd
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .map({
        case (k, v) => (v, k)
      })
      .sortByKey(false)
      .map({
        case (v, k) => (k, v)
      })
      .collect()
      .foreach(println)
    sc.stop()


  }
}
