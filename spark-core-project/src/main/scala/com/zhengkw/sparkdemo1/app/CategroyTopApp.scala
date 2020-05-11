package com.zhengkw.sparkdemo1.app

import com.zhengkw.sparkdemo1.acc.MyCategroyAcc
import com.zhengkw.sparkdemo1.entity.{CategroyCount, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * @ClassName:CategroyTopApp
 * @author: zhengkw
 * @description:
 * @date: 20/05/11下午 2:11
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object CategroyTopApp {
  /**
   * @descrption:
   * @param sc 因为使用累加器需要注册累加器！sc.register（acc，name）
   * @param actionRDD
   * @return: void
   * @date: 20/05/11 下午 2:14
   * @author: zhengkw
   */
  def showTop10(sc: SparkContext, actionRDD: RDD[UserVisitAction]) = {
    // 1. 创建累加器对象
    val acc = new MyCategroyAcc
    // 2. 注册累加器
    sc.register(acc, "myAcc")
    // 3. 遍历RDD, 进行累加
    actionRDD.foreach(action => acc.add(action))
    // 4. 进数据进行处理   top10
    val map: mutable.Map[String, (Long, Long, Long)] = acc.value

    val categoryList = map.map {
      case (cid, (click, order, pay)) =>
        CategroyCount(cid, click, order, pay)
    }.toList

    val result = categoryList
      .sortBy(x => (-x.clickCount, -x.orderCount, -x.payCount))
      .take(10)
    //result.foreach(println)
     result
  }
}
