package com.zhengkw.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:AggregateDemo
 * @author: zhengkw
 * @description:
 * @date: 20/05/06上午 10:59
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object AggregateDemo {
  def main(args: Array[String]): Unit = {
   /* val conf: SparkConf = new SparkConf().setAppName("FoldByKey").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
    var max = Int.MinValue
    var min = Int.MaxValue
    /*  val rdd2 = rdd.aggregateByKey(max)((u, v) => u.max(v), (max1, max2) => max1 + max2)
      val rdd3 = rdd.aggregateByKey(min)((x, y) => x.min(y), (min1, min2) => min1 + min2)
      rdd2.collect().foreach(println)
      rdd3.collect().foreach(println)*/
    //    val rdd3 = rdd.aggregateByKey(0)((m, n) => (m + n), (avg1, avg2) => (avg1 + avg2) / 2)
    val rdd3 = rdd.combineByKey(
      (v: Int) => (v, 1), // v表示和  1表示个数
      {
        case ((sum, count), value:Int) => (sum + value, count + 1)
      }, {
        case ((sum1: Int, count1: Int), (sum2: Int, count2: Int)) => (sum1 + sum2, count1 + count2)
      }
      /*(sumCount: (Int, Int), value: Int) =>
        (sumCount._1 + value, sumCount._2 + 1)
      ,
      (sumCount1: (Int, Int), sumCount2: (Int, Int)) =>
        (sumCount1._1 + sumCount2._1, sumCount1._2 + sumCount2._2)
      */
    )
*/

//   / rdd3.collect().foreach(println)
  }
}