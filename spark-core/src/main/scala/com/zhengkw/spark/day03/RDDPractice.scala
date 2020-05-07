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
    /*
            => 元数据做map
            => RDD((pro, ads), 1)  reduceByKey
            => RDD((pro, ads), count)   map
            => RDD(pro -> (ads, count), ....)      groupByKey
            => RDD( pro1-> List(ads1->100, abs2->800, abs3->600, ....),  pro2 -> List(...) )  map: 排序,前3
            => RDD( pro1-> List(ads1->100, abs2->800, abs3->600),  pro2 -> List(...) )
             */
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
      .reduceByKey(_ + _)
      .map({
        case (k, v) => (v, k)
      }).sortByKey(false)
      .map({
        case (k, v) => (v, k)
      })
      .map({
        case ((pro, ads), count) => pro -> (ads -> count)
      })
      .groupByKey()
      .map({
        case (k, v) => (k, v.take(3))
      }).sortByKey()
      .collect().foreach(println)


  }
}
