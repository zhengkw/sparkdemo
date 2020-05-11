package com.zhengkw.spark.day05

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2

/**
 * @ClassName:CustomAcc
 * @author: zhengkw
 * @description:
 * @date: 20/05/10下午 11:58
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object CustomAcc {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Acc1").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    val list1 = List(30, 50, 70, 60, 10, 20)
    val rdd1: RDD[Int] = sc.parallelize(list1, 2)
  }
}

class MyAcc extends AccumulatorV2 {
  private var _map = Map[String, Double]()

  //判断zero缓存是否为空
  override def isZero: Boolean = _map.isEmpty

  //复制累加器，将来用于序列化的时候告诉别人如何复制累加器！
  //有个分区算错了，那么就将这个计算计划给另一个分区，这个时候也会将累加器复制过去！
  //如果算了一部分了，那么要返回这个算好的计算结果
  override def copy(): AccumulatorV2[Nothing, Nothing] = ???

  //重置累加器
  override def reset(): Unit = ???

  //分区内
  override def add(v: Nothing): Unit = ???

  //分区间
  override def merge(other: AccumulatorV2[Nothing, Nothing]): Unit = ???

  //返回值
  override def value: Nothing = ???
}