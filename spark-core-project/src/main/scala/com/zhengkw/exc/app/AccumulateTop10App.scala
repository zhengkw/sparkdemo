package com.zhengkw.exc.app

import com.zhengkw.exc.entity.UserVisitAction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @ClassName:AccumulateTop10
 * @author: zhengkw
 * @description:
 * @date: 20/05/11下午 10:20
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object AccumulateTop10App {
  def accumlate(sc: SparkContext, userActionRdd: RDD[UserVisitAction]) = {
    //new一个累加器

    // 注册累加器
    //计算
  }
}
