package com.zhengkw.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
 * @ClassName:SortBy
 * @author: zhengkw
 * @description:
 * @date: 20/05/07下午 8:23
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object SortBy {
  def main(args: Array[String]): Unit = {
    val list = List("a", "b", "c", "aa", "ab", "ba", "adaf", "abc", "abd")
    val sc = new SparkContext(new SparkConf().setMaster("local[2]")
      .setAppName("sortbydemo"))
    val rdd = sc.parallelize(list, 2)
    //根据长度排序
    val rdd2 = rdd.sortBy(_.length, false)
    rdd2.collect().foreach(println)
    //根据长度降序，字典降序
    rdd.sortBy((x => (x.length, x)), false)
    //长度升序，字典降序 利用ordering加classtag
    val rdd3 = rdd.sortBy((x => (x.length, x)), true)(Ordering
      .Tuple2(Ordering.Int, Ordering.String.reverse), ClassTag(classOf[(Int, String)]))
    /*  val rdd3 = rdd.sortBy((x => (x.length, x)), true)(Ordering.Tuple2(Ordering.Int, Ordering.String.reverse)
        , ClassTag(classOf[Tuple2[Int, String]]))*/
    rdd3.collect().foreach(println)
  }
}