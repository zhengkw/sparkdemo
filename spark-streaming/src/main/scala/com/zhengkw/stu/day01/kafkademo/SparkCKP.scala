package com.zhengkw.stu.day01.kafkademo

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName:SparkCKP
 * @author: zhengkw
 * @description: 检查点恢复！ 直连模式 严格一次输出！
 * @date: 20/05/17下午 10:05
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object SparkCKP {

  def createFun(): StreamingContext = {
    val conf = new SparkConf()
      .setMaster("local[*]").setAppName("SparkCKP")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    //必须声明！
    ssc.checkpoint("./ck1") // 对ssc做checkpoint.
    /* //非直连模式通过实现WAL来避免丢失数据，或者用ckp！下面的是ckp来保证输出严格一次！
     val rids: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
       ssc,
       "hadoop102:2181,hadoop103:2181,hadoop104:2181/mykafka",
       "zhengkw",
       Map("sparktest" -> 2)
     )
     rids
       .map(_._2)
       .flatMap(_.split(" "))
       .map((_, 1))
       .reduceByKey(_ + _)
       .print
     ssc*/

    //直连利用ckp
    val param = Map[String, String](
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "group.id" -> "zhengkw"
    )
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      param,
      Set("sparktest"))
    stream
      .map(_._2)
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print

    ssc
  }

  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getActiveOrCreate("./ck1", createFun _)
    ssc.start()
    ssc.awaitTermination()
  }
}
