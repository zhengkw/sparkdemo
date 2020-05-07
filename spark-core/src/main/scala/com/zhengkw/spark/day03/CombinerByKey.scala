package com.zhengkw.spark.day03

import com.sun.javaws.Main
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:CombinerByKey
 * @author: zhengkw
 * @description:
 * @date: 20/05/07下午 10:53
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object CombinerByKey {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf().setMaster("local[2]").setAppName("combiner"))
    val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 5)), 2)
    val rdd2 = rdd
      .combineByKey(
        v => (v, 1),//zero
        (sumcount: (Int, Int), value: Int) => (sumcount._1 + value, sumcount._2 + 1), //内聚合
        (sumcount1: (Int, Int), sumcount2: (Int, Int)) => (sumcount1._1 + sumcount2._1, sumcount1._2 + sumcount2._2)//分区间聚合
      )
    rdd2.collect().foreach(println)
  }
}
