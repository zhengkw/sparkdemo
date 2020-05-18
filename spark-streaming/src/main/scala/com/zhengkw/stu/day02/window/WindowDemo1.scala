package com.zhengkw.stu.day02.window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName:WindowDemo1
 * @author: zhengkw
 * @description: invReduceFun 参数使用
 * @date: 20/05/18下午 12:04
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object WindowDemo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WindowDemo")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("./ck2")
    val stream = ssc.socketTextStream("hadoop102", 9876)
    stream
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKeyAndWindow(_ + _, invReduceFunc = (now, pre) => now - pre, windowDuration = Seconds(9), slideDuration = Seconds(3), filterFunc = (_._2 > 0))
      .print(100)
    ssc.start()
    ssc.awaitTermination()
  }
}
