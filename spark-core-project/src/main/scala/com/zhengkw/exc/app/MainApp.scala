package com.zhengkw.exc.app

import com.zhengkw.exc.entity.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:MainApp
 * @author: zhengkw
 * @description:
 * @date: 20/05/13下午 9:02
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object MainApp {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf()
      .setMaster("local[2]")
      .setAppName("MainApp"))
    val sourceRdd = sc.textFile("E:\\IdeaWorkspace\\sparkdemo\\data\\user_visit_action.txt")
    val useractionRdd: RDD[UserVisitAction] = sourceRdd.map(line => {
      val splits = line.split("_")
      UserVisitAction(
        splits(0),
        splits(1).toLong,
        splits(2),
        splits(3).toLong,
        splits(4),
        splits(5),
        splits(6).toLong,
        splits(7).toLong,
        splits(8),
        splits(9),
        splits(10),
        splits(11),
        splits(12).toLong)
    })
    //useractionRdd.collect.foreach(println)

    //调用外部的方法进行处理
    CategoryTop10.showtop10(sc, useractionRdd)
  }
}
