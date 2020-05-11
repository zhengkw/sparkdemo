package com.zhengkw.sparkdemo1.app

import com.zhengkw.sparkdemo1.entity.UserVisitAction
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @ClassName:Demo1App
 * @author: zhengkw
 * @description:
 * @date: 20/05/11下午 2:00
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object Demo1App {
  /**
   * @descrption: 入口方法，获取文件内容，进行top10统计
   *              统计指标 点击量，下单量，支付量！
   * @param args
   * @return: void
   * @date: 20/05/11 下午 2:01
   * @author: zhengkw
   */
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ProjectApp").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    // 1. 读数据
    val sourceRDD: RDD[String] = sc.textFile("E:\\IdeaWorkspace\\sparkdemo\\data\\user_visit_action.txt")
    //处理数据
    val userVisitActionRDD = sourceRDD.map(line => {
      val splits: Array[String] = line.split("_") //按下划线切分
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
    //计算Top10
    CategroyTopApp.showTop10(sc, userVisitActionRDD)
    sc.stop()
  }
}
