package com.zhengkw.spark.streaming.project.app

import com.zhengkw.spark.streaming.project.bean.AdsInfo
import com.zhengkw.spark.streaming.project.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * Author atguigu
 * Date 2020/5/18 15:32
 */
object AreaAdsTopApp {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AreaAdsTopApp")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("./ck1")

    val adsInfoStream = MyKafkaUtil
      .getKafkaStream(ssc, "ads_log")
      .map(log => {
        val splits: Array[String] = log.split(",")
        // 1589787737517,华南,深圳,104,4
        AdsInfo(splits(0).toLong, splits(1), splits(2), splits(3), splits(4))
      })

    // 需求分析:
    /*

        DStream[(day, area, ads), 1]  updateStateByKey
        DStream[(day, area, ads), count]

        分组, top 3
        DStream[(day, area), (ads, count)]
        DStream[(day, area), it[(ads, count)]]
        排序取30

     */
    adsInfoStream
      .map(info => ((info.dayString, info.area, info.adsId), 1))
      .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => {
        Some(seq.sum + opt.getOrElse(0))
      })
      .map {
        case ((day, area, ads), count) =>
          ((day, area), (ads, count))
      }
      .groupByKey
      .mapValues(it => {
        it.toList.sortBy(-_._2).take(3)
      })
      .print


    ssc.start()
    ssc.awaitTermination()

  }
}

/*
要么实时, 要么离线(批处理)
实时:
    难度小, 指标也少
离线:
    难度大, 指标也多
    
    
每天每地区热门广告 Top3

DStream[(day, area, ads), 1]  updateStateByKey
DStream[(day, area, ads), count]

分组, top 3
DStream[(day, area), (ads, count)]
DStream[(day, area), it[(ads, count)]]
排序取30




 */
