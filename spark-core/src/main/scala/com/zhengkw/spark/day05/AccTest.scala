package com.zhengkw.spark.day05

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @ClassName:AccTest
 * @author: zhengkw
 * @description:
 * @date: 20/05/10下午 11:51
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object AccTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Acc1").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    val list1 = List(30, 50, 70, 60, 10, 20)
    val rdd1: RDD[Int] = sc.parallelize(list1, 2)
    val acc = sc.longAccumulator
    rdd1.foreach(x => {
      acc.add(1)
      x
    })
    println(acc.value)
  }
}
