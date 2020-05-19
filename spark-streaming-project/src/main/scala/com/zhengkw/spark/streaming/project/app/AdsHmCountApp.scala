package com.zhengkw.spark.streaming.project.app

import com.zhengkw.spark.streaming.project.bean.AdsInfo
import com.zhengkw.spark.streaming.project.util.MyJDBCUtil
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.Serialization

/**
 * @ClassName:AdsHmCountApp
 * @author: zhengkw
 * @description:
 * @date: 20/05/19上午 10:18
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object AdsHmCountApp extends BaseApp {
  //ads_click_perminInhour
  val sql = "" +
    "insert into " +
    "ads_click_perminInhour " +
    "values(?,?) on duplicate key update info=?"

  /**
   * @descrption: 抽象方法，子类实现业务代码
   *              将每分钟的点击量聚合以后通过foreachrdd 写到mysql
   * @param ssc
   * @param adsInfoStream
   * @return: void
   * @date: 20/05/19 上午 10:28
   * @author: zhengkw
   */
  override def optionTask(ssc: StreamingContext, adsInfoStream: DStream[AdsInfo]): Unit = {
    //((adsid,hm),1)
    val ds = adsInfoStream.window(Minutes(60), Seconds(6))
      .map(info => ((info.adsId, info.hmString), 1))
      //((adsid,hm),count)
      .reduceByKey(_ + _)
      //(ads, (hm, count))
      .map({
        case ((ads, hm), count) => (ads, (hm, count))
      })
      //将ads相同的划分为一个组
      .groupByKey()
    //将结果输出到mysql
    ds.foreachRDD(rdd => {
      //对每个rdd分区进行操作 对外输出用FP
      rdd.foreachPartition(it => {

        //获取数据库连接对象
        val conn = MyJDBCUtil.getConn()
        //执行sql语句 ads_click_perminInhour
        val ps = conn.prepareStatement(sql)
        it.foreach({
          case (adsId, it) => {
            ps.setInt(1, adsId.toInt)
            //格式化成json
            import org.json4s.DefaultFormats
            val jsonStr = Serialization.write(it.toMap)(DefaultFormats)
            ps.setString(2, jsonStr)
            ps.setString(3, jsonStr)
            ps.execute()
            //关闭对象
            ps.close()
          }
        })

      })
    })


  }
}
