package com.zhengkw.spark.day03

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @ClassName:RDDPractice
 * @author: zhengkw
 * @description:
 * @date: 20/05/08上午 12:43
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object RDDPractice {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDDPractice").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    // 1. 读取原始数据 1516609143867 6 7 64 16 时间戳 省 城市 用户 广告
    val lineRDD: RDD[String] = sc.textFile("E:\\BaiduNetdiskDownload/agent.log")
    val proAndAdsRdd = lineRDD.map({
      line =>
        val splits = line.split(" ")
        //(（省, 广告）,count)
        //((splits(1), splits(4), 1))
        ((splits(1), splits(4)), 1)
    })
      .reduceByKey(_ + _) //把前2个看成一个key 集合count
      .map({
        case (k, v) => (v, k)
      }).sortByKey(false) //通过key排序count所以交换位置！
      .map({
        case (k, v) => (v, k)
      })
      .map({
        case ((pro, ads), count) => pro -> (ads -> count) // 让省作为key
      })
      .groupByKey()
      .map({
        case (k, v) => (k, v.take(3)) //取count的前3
      }).sortByKey()
      .collect().foreach(println)
  }
}
