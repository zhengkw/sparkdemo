package com.zhengkw.spark.streaming.project.review

import com.zhengkw.spark.streaming.project.bean.AdsInfo
import com.zhengkw.spark.streaming.project.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName:BaseAdsInfoApp
 * @author: zhengkw
 * @description:
 * @date: 20/05/19下午 4:53
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
trait BaseAdsInfoApp {
  /**
   * @descrption: 入口
   * @param args
   * @return: void
   * @date: 20/05/19 下午 4:55
   * @author: zhengkw
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("BaseAdsInfoApp")
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))
    //设置 ckp
    ssc.checkpoint("./ck1")
    //从kafka中读取数据
    val ds = MyKafkaUtil.getKafkaStream(ssc, "ads_log")
    //ds读取的是一行数据 一行数据不是集合，是一个字符串 所以不用展开
    val data: DStream[AdsInfo] = ds.map({
      line =>
        val splits = line.split(",")
        // println("---------\t"+splits(0))
        // 1589787737517,华南,深圳,104,4
        AdsInfo(splits(0).toLong,
          splits(1),
          splits(2),
          splits(3),
          splits(4))
    })
    doSomething(data)
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * @descrption: 业务逻辑抽象类 子类实现具体业务
   * @param data 本特质解析的kafka数据源 封装成一个DS【caseclass】
   *             对该数据进行业务处理
   * @return: void
   * @date: 20/05/19 下午 5:12
   * @author: zhengkw
   */
  def doSomething(data: DStream[AdsInfo]): Unit
}
