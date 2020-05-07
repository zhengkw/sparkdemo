package com.zhengkw.spark.day02Exc.doublevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:ZipPractice
 * @author: zhengkw
 * @description:
 * @date: 20/05/07下午 3:12
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object ZipPractice {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ZipPractice").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list1 = List(30, 50, 70, 60, 10, 20)
    // rdd:  "30->50" , "50->70",  "70->60", "60->10", "10->20"
    val rdd1 = sc.parallelize(list1.init, 2)
    val rdd2 = sc.parallelize(list1.tail, 2)
    val rdd3 = rdd1.zip(rdd2)
    // rdd3.collect().foreach(println)
    val rdd4 = rdd3.map(
      {
        case (v1, v2) => s"$v1 -> $v2  "
      }
    )
    println(rdd4.collect().toList.mkString(","))
  }
}
