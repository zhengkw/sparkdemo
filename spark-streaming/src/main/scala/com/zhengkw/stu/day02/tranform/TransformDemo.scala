package com.zhengkw.stu.day02.tranform

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName:TranformDemo
 * @author: zhengkw
 * @description:
 * @date: 20/05/18上午 10:18
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object TransformDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("TransformDemo")
    val ssc = new StreamingContext(conf, Seconds(3))
    val stream = ssc.socketTextStream("hadoop102", 9876)
    // 把对流的操作, 转换成对RDD操作
    val result = stream.transform(rdd => {
      rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    } )
    result.print
    ssc.start()
    ssc.awaitTermination()
  }
}
