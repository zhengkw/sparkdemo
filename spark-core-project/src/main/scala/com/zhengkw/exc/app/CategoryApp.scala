package com.zhengkw.exc.app

import com.zhengkw.exc.entity.UserVisitAction
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:CategoryApp
 * @author: zhengkw
 * @description:
 * @date: 20/05/11下午 10:00
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object CategoryApp {
  def main(args: Array[String]): Unit = {
    //读取文件数据
    val sc = new SparkContext(new SparkConf()
      .setAppName("capp").setMaster("local[2]"))
    val sourceRDD = sc.textFile("E:\\IdeaWorkspace\\sparkdemo\\data\\user_visit_action.txt")
    //处理读取到的数据每次处理一行
    val userActionRdd = sourceRDD.map(line => {
      val propertys = line.split("_")
      //将数据封装到一个样例类中，然后返回一个RDD【样例类】
      UserVisitAction(propertys(0),
        propertys(1).toLong,
        propertys(2),
        propertys(3).toLong,
        propertys(4),
        propertys(5),
        propertys(6).toLong,
        propertys(7).toLong,
        propertys(8),
        propertys(9),
        propertys(10),
        propertys(11),
        propertys(12).toLong
      )
    })
    //计算top10
    //传一个sc用于注册累加器，传RDD数据源
    AccumulateTop10App.accumlate(sc, userActionRdd)

    sc.stop()
  }
}
