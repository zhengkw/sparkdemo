/*
package com.zhengkw.stu.day01.kafkademo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName:ReceiverApi
 * @author: zhengkw
 * @description: 没有输出严格一次！
 * @date: 20/05/17下午 9:14
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object ReceiverApi {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ReceiverApi")
    val ssc = new StreamingContext(conf, Seconds(3))
    val rids = KafkaUtils.createStream(
      ssc,
      "hadoop102:2181,hadoop103:2181,hadoop104:2181/mykafka",
      "zhengkw",
      Map("sparktest" -> 2)
    )
    //获取数据进行wordcount 输出KV 一般K为NULL
    val ds = rids.flatMap(_._2.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
    ds.print(1000)

    ssc.start()
    ssc.awaitTermination()
  }
}
*/
