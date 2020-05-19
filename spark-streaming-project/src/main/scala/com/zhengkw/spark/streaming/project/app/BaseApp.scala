package com.zhengkw.spark.streaming.project.app

import com.zhengkw.spark.streaming.project.bean.AdsInfo
import com.zhengkw.spark.streaming.project.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName:BaseApp
 * @author: zhengkw
 * @description:
 * @date: 20/05/19上午 10:18
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
trait BaseApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("BaseApp ")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("./ck1")
    val adsInfoStream: DStream[AdsInfo] = MyKafkaUtil
      .getKafkaStream(ssc, "ads_log")
      .map(log => {
        val splits: Array[String] = log.split(",")
        // 1589787737517,华南,深圳,104,4
        AdsInfo(splits(0).toLong, splits(1), splits(2), splits(3), splits(4))
      })

    //不同的业务处理方法不一样
    optionTask(ssc, adsInfoStream)

    ssc.start()
    ssc.awaitTermination()
  }
  /**
   * @descrption: 抽象方法，子类实现业务代码
   * @param ssc
   * @param adsInfoStream
   * @return: void
   * @date: 20/05/19 上午 10:28
   * @author: zhengkw
   */
  def optionTask(ssc: StreamingContext, adsInfoStream: DStream[AdsInfo])

}
