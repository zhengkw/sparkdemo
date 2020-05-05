package com.zhengkw.spark.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:Sum
 * @author: zhengkw
 * @description:
 * @date: 20/05/05上午 11:49
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object Sum {
  def main(args: Array[String]): Unit = {
    val list1 = List(30, 5, 7, 6, 1, 20)
    val conf = new SparkConf().setMaster("local[2]").setAppName("Sum")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(list1, 2)
    val rdd2 = rdd1.groupBy(x => x % 2)
    val rdd3 = rdd2.map {
      case (index, it) => (index, it.sum)
    }
    rdd3.collect()
    rdd3.foreach(println)

    sc.stop()
  }
}
