package com.zhengkw.sparkdemo1.app

import com.zhengkw.sparkdemo1.entity.{CategroyCount, SessionInfo, UserVisitAction}
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

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
    //cid集合拿出来 cid是long 但是传过来的是string
    val cids = categoryCountList.map(_.cid.toLong)
    //将包含需求一的top10种类过滤出来
    val topCategoryActionRDD = userVisitActionRDD.filter(x => cids.contains(x.click_category_id))
    //  topCategoryActionRDD.collect().foreach(print)
    //计算每个品类 下的每个session 的点击量  rdd ((cid, sid) ,1)
    val cidAndSessionRdd = topCategoryActionRDD.map({
      case action => ((action.click_category_id, action.session_id), 1)
    })
    //cidAndSessionRdd.collect().foreach(print)
    val cidAndSessionCountRdd = cidAndSessionRdd
      .reduceByKey(_ + _)
      .map({
        case ((cid, sid), count) => (cid, (sid, count))
      })
    //根据cid分组
    val cidAndSidCountGrouped = cidAndSessionCountRdd.groupByKey()
    //排序
    val result = cidAndSidCountGrouped.map({
      case (cid, sidcountIt) => (cid, sidcountIt.toList.sortBy(-_._2).take(10))
    })
    result.collect.foreach(println)
  }


  def statCategoryTop10Session_1(sc: SparkContext,
                                 categoryCountList: List[CategroyCount],
                                 userVisitActionRDD: RDD[UserVisitAction]) = {
    //cid集合拿出来 cid是long 但是传过来的是string
    val cids = categoryCountList.map(_.cid.toLong)
    //将包含需求一的top10种类过滤出来
    val topCategoryActionRDD = userVisitActionRDD.filter(x => cids.contains(x.click_category_id))
    //  topCategoryActionRDD.collect().foreach(print)
    //计算每个品类 下的每个session 的点击量  rdd ((cid, sid) ,1)
    val cidAndSessionRdd = topCategoryActionRDD.map({
      case action => ((action.click_category_id, action.session_id), 1)
    })
    //cidAndSessionRdd.collect().foreach(print)
    val cidAndSessionCountRdd = cidAndSessionRdd
      .reduceByKey(new myPartitioner(cids), _ + _)
      .map({
        case ((cid, sid), count) => (cid, (sid, count))
      })
    // 3. 排序取top10
    val result = cidAndSessionCountRdd.mapPartitions((it: Iterator[(Long, (String, Int))]) => {

      var treeSet: mutable.Set[SessionInfo] = mutable.TreeSet[SessionInfo]()
      var id = 0L
      it.foreach {
        case (cid, (sid, count)) =>
          id = cid
          treeSet += SessionInfo(sid, count)
          if (treeSet.size > 10) treeSet = treeSet.take(10)
      }
      //            treeSet.toIterator.map((id, _))
      Iterator((id, treeSet.toList.map({
        case sessionInfo: SessionInfo => (sessionInfo.sid,sessionInfo.count)
      })))
    })

    result.collect.foreach(println)

  }


  class myPartitioner(cids: List[Long]) extends Partitioner {
    private val map: Map[Long, Int] = cids.zipWithIndex.toMap

    override def numPartitions: Int = cids.size

    override def getPartition(key: Any): Int =
      key match {
        case (cid: Long, sid) => map(cid)
      }
  }

}
