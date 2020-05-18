package com.zhengkw.stu.day02.kafkademo

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{ConsumerStrategy, KafkaUtils, LocationStrategies, LocationStrategy, PerPartitionConfig}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName:KafkaConn
 * @author: zhengkw
 * @description: 0-10API wordCount
 * @date: 20/05/18上午 9:41
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object KafkaConn {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaConn")
    val ssc = new StreamingContext(conf, Seconds(3))
    val topics = Array("sparktest")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "key.deserializer" -> classOf[StringDeserializer], // key的反序列化器
      "value.deserializer" -> classOf[StringDeserializer], // value的反序列化器
      "group.id" -> "zhengkw",
      "auto.offset.reset" -> "latest", // 每次从最新的位置开始读
      "enable.auto.commit" -> (true: java.lang.Boolean) // 自动提交kafka的offset
    )
    val ds = KafkaUtils.createDirectStream[String, String](
      ssc: StreamingContext,
      locationStrategy = LocationStrategies.PreferConsistent, //平均分配
      Subscribe[String, String](topics, kafkaParams)
    )
    ds.flatMap((_.value().split(" ")))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()
    ssc.start()
    ssc.awaitTermination()
  }
}
