package com.zhengkw.spark.day02Exc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @ClassName:Simple
 * @author: zhengkw
 * @description: 抽样
 * @date: 20/05/06上午 12:39
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object Sample {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Sample").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list1 = List(30, 50, 70, 60, 10, 20, 1, 2, 3, 4)
    val rdd1: RDD[Int] = sc.parallelize(list1, 2)
         //种子一般不赋值，一般和时间戳有关！如果固定，那么随机数也是固定了！
    val rdd2: RDD[Int] = rdd1.sample(true, 0.5, 1)
    rdd2.collect.foreach(println)
    sc.stop()
  }
}
