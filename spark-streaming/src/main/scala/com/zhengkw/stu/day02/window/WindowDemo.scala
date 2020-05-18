package com.zhengkw.stu.day02.window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName:WindowDemo
 * @author: zhengkw
 * @description:
 * @date: 20/05/18上午 11:53
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object WindowDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("TransformDemo")
    val ssc = new StreamingContext(conf, Seconds(3))
    val stream = ssc.socketTextStream("hadoop102", 9876)
    stream.flatMap()
  }
}
