package com.zhengkw.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:Wordcount1
 * @author: zhengkw
 * @description:
 * @date: 20/05/08上午 12:04
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object Wordcount1 {
  def main(args: Array[String]): Unit = {
    var path = "F:\\mrinput\\wordcount\\test.txt"
    val conf: SparkConf = new SparkConf().setAppName("wordcount").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd = sc.textFile(path)
    //rdd.collect().foreach(println)
    val rdd1 = rdd.flatMap(_.split(" "))
    // value.collect().foreach(println)
    val rdd2 = rdd1
      .map((_, 1))
      .aggregateByKey(0)(_ + _, _ + _)
      //根据 出现频率降序排列
      .map({
        case (k, v) => (v, k)
      })
      .sortByKey(false)
      .map({
        case (k, v) => (v, k)
      })
    rdd2.collect().foreach(println)
  }
}
