package com.zhengkw.spark.streaming.project.review

import com.zhengkw.spark.streaming.project.bean.AdsInfo
import com.zhengkw.spark.streaming.project.util.MyJDBCUtil
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.Serialization

/**
 * @ClassName:NearOneHourClickPerMinToDB
 * @author: zhengkw
 * @description:
 * @date: 20/05/19下午 9:10
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object NearOneHourClickPerMinToDB extends BaseAdsInfoApp {
  //ads_click_perminInhour
  val sql = "insert into " +
    "ads_click_perminInhour " +
    "values(?,?) on duplicate key update info=?"


  /**
   * @descrption: 各广告最近 1 小时内各分钟的点击量输出到DB
   *              (ads,hm,1)
   *              (ads,(hm,count))
   * @param data 本特质解析的kafka数据源 封装成一个DS【caseclass】
   *             对该数据进行业务处理
   * @return: void
   * @date: 20/05/19 下午 5:12
   * @author: zhengkw
   */
  override def doSomething(data: DStream[AdsInfo]): Unit = {
    val ds = data.window(Minutes(60), Seconds(6))
      .map(data => ((data.adsId, data.hmString), 1))
      .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => {
        Some(seq.sum + opt.getOrElse(0))
      })
      .map({
        case ((ads, hm), count) => (ads, (hm, count))
      })
      .groupByKey()

    ds.foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        //获取conn
        val conn = MyJDBCUtil.getConn()
        //获取ps
        val ps = conn.prepareStatement(sql)
        //遍历it
        it.foreach({
          case (ads, it) => {
            ps.setInt(1, ads.toInt)
            import org.json4s.DefaultFormats
            val jsonStr = Serialization.write(it.toMap)(DefaultFormats)
            ps.setString(2, jsonStr)
            ps.setString(3, jsonStr)
            //执行
            ps.execute()
            //关闭资源
            MyJDBCUtil.closeConn()
          }
        })
      })
    })
  }
}
