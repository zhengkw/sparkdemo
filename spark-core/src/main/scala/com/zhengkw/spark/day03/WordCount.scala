package com.zhengkw.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
 * @ClassName:WordCount
 * @author: zhengkw
 * @description:
 * @date: 20/05/06下午 2:25
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    var path = "F:\\mrinput\\wordcount"
    //获取文件流
    val file = Source.fromFile(path, "utf-8")
    //获取一行
    val stringLines = file.getLines()
    val strings = stringLines.flatMap(x => x.split(" ")).filter(x => x.trim.length > 0)
    val map = strings.toList.groupBy(x => x).mapValues(_.size)
    val conf: SparkConf = new SparkConf().setAppName("FoldByKey").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd = sc.parallelize(map.toList, 2)
    val rdd1 = rdd.groupByKey()
  }
}
