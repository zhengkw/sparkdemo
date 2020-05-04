package com.zhengkw.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:WordCount
 * @author: zhengkw
 * @description:
 * @date: 20/05/04下午 9:58
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordcount")
    //获取sc
    val sc = SparkContext.getOrCreate(conf)
    // 2. 通过sc从数据源得到数据, 第一个RDD  (文件地址从main函数传递)
    val lineRDD: RDD[String] = sc.textFile(args(0))
    // 3. 对RDD做各种转换
    val wordCountRDD = lineRDD
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
    // 4. 执行一个行动算子(collect: 把每个executor中执行的结果, 收集到driver端)
    val arr: Array[(String, Int)] = wordCountRDD.collect
    arr.foreach(println)
    // 5. 关闭SparkContext
    sc.stop()
  }
}
