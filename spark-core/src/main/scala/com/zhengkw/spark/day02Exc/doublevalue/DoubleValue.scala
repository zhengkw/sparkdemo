package com.zhengkw.spark.day02Exc.doublevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:DoubleValue
 * @author: zhengkw
 * @description:
 * @date: 20/05/06下午 11:01
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object DoubleValue {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DoubleVlaue1").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list1 = List(30, 50, 70, 60)
    val list2 = List(3, 5, 7, 6, 1, 2)
    val rdd1 = sc.parallelize(list1, 2)
    val rdd2 = sc.parallelize(list2, 2)
    //交集
    rdd1.intersection(rdd2)
    //并集
    rdd1.union(rdd2)
    rdd1 ++ rdd2
    //差集
    rdd1.subtract(rdd2)
    //笛卡尔积
    rdd1.cartesian(rdd2)
  }
}
