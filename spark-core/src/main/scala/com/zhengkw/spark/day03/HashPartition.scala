package com.zhengkw.spark.day03

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @ClassName:HashPartition
 * @author: zhengkw
 * @description:
 * @date: 20/05/07下午 4:25
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object HashPartition {
  def main(args: Array[String]): Unit = {
    val list = List("hawo", "zhengkw", "hi", "98k", "hi", "world", "zhengkw")
    val sc = new SparkContext(
      new SparkConf()
        .setAppName("hashpartition")
        .setMaster("local[2]"))
    val rdd1 = sc.parallelize(list, 2)
    val rdd2 = rdd1.map((_, 1))
    val rdd3 = rdd2.map({
      case (k, v) => (v, k)
    })
    val rdd4 = rdd3.partitionBy(new HashPartitioner(2)).map({
      case (v, k) => (k, v)
    })
    rdd4.glom().map(_.toList).collect().foreach(print)
  }
}
