package com.zhengkw.spark.day02Exc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:MapDemo
 * @author: zhengkw
 * @description:
 * @date: 20/05/05下午 9:54
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object MapDemo {
  def main(args: Array[String]): Unit = {
    val list1 = List(30, 50, 70, 60, 10, 20)
    val sc = new SparkContext(new SparkConf().setAppName("mapdemo").setMaster("local[2]"))
    val rdd1 = sc.parallelize(list1, 2)
    //将数组元素转换成元组
    val rdd2 = rdd1.map(x => (x * 2, x + 3))
    rdd2.collect().foreach(print)
  }
}
