package com.zhengkw.sparkdemo1.app

import com.zhengkw.sparkdemo1.entity.{CategroyCount, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @ClassName:CategorySessionTopApp
 * @author: zhengkw
 * @description:
 * @date: 20/05/11下午 11:21
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object CategorySessionTopApp {
  def statCategoryTop10Session(sc: SparkContext,
                               categoryCountList: List[CategroyCount],
                               userVisitActionRDD: RDD[UserVisitAction]) = {
    //cid集合拿出来
    val cids = categoryCountList.map(_.cid)
    //将包含需求一的top10种类过滤出来
    val topCategoryActionRDD = userVisitActionRDD.filter(x => cids.contains(x.click_category_id))
    //计算每个品类 下的每个session 的点击量  rdd ((cid, sid) ,1)
    val cidAndSessionRdd = topCategoryActionRDD.map({
      case action => ((action.click_category_id, action.session_id), 1)
    })
    val cidAndSessionCountRdd = cidAndSessionRdd
      .reduceByKey(_ + _)
      .map({
        case ((cid, sid), count) => (cid, (sid, count))
      })
    //根据cid分组
    val cidAndSidCountGrouped = cidAndSessionCountRdd.groupByKey()
    //排序
    val result = cidAndSidCountGrouped.map({
      case (cid, sidcountIt) => (cid, sidcountIt.toList.sortBy(-_._2))
    })
    result.collect.foreach(println)
  }

}
