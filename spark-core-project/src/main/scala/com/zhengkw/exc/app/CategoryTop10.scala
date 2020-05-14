package com.zhengkw.exc.app

import com.zhengkw.exc.entity.UserVisitAction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @ClassName:CategoryTop10
 * @author: zhengkw
 * @description:
 * @date: 20/05/13下午 9:50
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object CategoryTop10 {
  def showtop10(sc: SparkContext, useractionRdd: RDD[UserVisitAction]) = {
    //创建一个累加器

  }
}
