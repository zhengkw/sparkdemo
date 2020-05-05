package com.zhengkw.spark.day02Exc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @ClassName:GroupBy
 * @author: zhengkw
 * @description:
 * @date: 20/05/06上午 12:07
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object GroupBy {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupBy").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list1 = List(3, 50, 7, 6, 1, 20)
    val rdd1: RDD[Int] = sc.parallelize(list1, 2)
    //奇数偶数分组
    val rdd2: RDD[(Int, Iterable[Int])] = rdd1.groupBy(x => x % 2)
    // 计算这个集合中, 所有的奇数的和偶数的和
    val rdd3 = rdd2.map {
     //偏函数匹配！
      case (index, it) => it.sum
    }
    rdd3.collect().foreach(println)
  }
}
