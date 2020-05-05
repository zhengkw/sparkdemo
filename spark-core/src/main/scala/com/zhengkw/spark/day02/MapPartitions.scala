package com.zhengkw.spark.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:MapPartitions
 * @author: zhengkw
 * @description:
 * @date: 20/05/05上午 10:46
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object MapPartitions {
  def main(args: Array[String]): Unit = {
    val list1 = List(30, 50, 70, 60, 10, 20) // 两个分区
    val conf = new SparkConf().setMaster("local[*]").setAppName("MapPartition")
    val sc = new SparkContext(conf)
    //转换成RDD
    val rdd1 = sc.parallelize(list1)
    val rdd2 = rdd1.map(_ * 2)

    rdd1.collect() //100  20  140   60   120   40
    //rdd2.collect() //120   60   20   100   40  140
    /* val rdd2 = rdd1.mapPartitions(it => {
       it.map(_ * 2)
     })

     // rdd1.collect()//100   120    140    40    20    60
     rdd2.collect() //140    120    100    40    20    60
    */
    rdd2.foreach(println)
    sc.stop()

  }

}
