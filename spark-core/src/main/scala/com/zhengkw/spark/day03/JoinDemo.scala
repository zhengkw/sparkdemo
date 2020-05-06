package com.zhengkw.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:JoinDemo
 * @author: zhengkw
 * @description:
 * @date: 20/05/06下午 3:12
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object JoinDemo {
  def main(args: Array[String]): Unit = {
    val list1 = List((2, "ad"), (3, "asda"), (4, "afeadf"), (5, "asda"))
    val list2 = List((2, "adc"), (3, "asdac"), (5, "asda"))
    val conf: SparkConf = new SparkConf().setAppName("join").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1 = sc.parallelize(list1, 2)
    val rdd2 = sc.parallelize(list2, 2)
    val value = rdd1.join(rdd2)
    val value1 = rdd1.leftOuterJoin(rdd2)
    val value2 = rdd1.rightOuterJoin(rdd2)
    val value3 = rdd1.fullOuterJoin(rdd2)
    value.collect().foreach(println)
    println("_____________________")
    value1.collect().foreach(println)
    println("_____________________")
    value2.collect().foreach(println)
    println("_____________________")
    value3.collect().foreach(println)
  }
}
