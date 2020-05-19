package com.zhengkw.spark.streaming.project.review

import com.zhengkw.spark.streaming.project.bean.AdsInfo
import org.apache.spark.streaming.dstream.DStream

import scala.Some

/**
 * @ClassName:AreaAdsTOP3App
 * @author: zhengkw
 * @description:
 * @date: 20/05/19下午 7:14
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object AreaAdsTOP3App extends BaseAdsInfoApp {
  /**
   * @descrption: 实现每个地区的商品点击前3
   * @param data 本特质解析的kafka数据源 封装成一个DS【caseclass】
   *             对该数据进行业务处理
   * @return: void
   * @date: 20/05/19 下午 5:12
   * @author: zhengkw
   */
  override def doSomething(data: DStream[AdsInfo]): Unit = {
    // 1589787737517,华南,深圳,104,4
    data.map(
      line => ((line.area, line.adsId, line.dayString), 1))
      .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => {
        Some(seq.sum + opt.getOrElse(0))
      }).map({
      case ((area, ads, day), count) => ((day, area), (ads, count))
    })
      .groupByKey()
      .mapValues(it => it.toList
        .sortBy(-_._1.toInt).take(3))
      .print(1000)
  }
}
