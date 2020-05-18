package com.zhengkw.stu.day02.window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName:WindowDemo2
 * @author: zhengkw
 * @description:
 * @date: 20/05/18下午 12:12
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object WindowDemo2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TransformDemo")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("./ck2")
    // 直接给DStream分配窗口, 将来所有的操作, 都是基于窗口
    val lineStream =
      ssc.socketTextStream("hadoop102", 9876).window(Seconds(9), Seconds(6))
    // 把对流的操作, 转换成对RDD操作.
    val result = lineStream
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    result.print

    ssc.start()
    ssc.awaitTermination()
  }
}
