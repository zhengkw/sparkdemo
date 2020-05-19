package com.zhengkw.spark.streaming.project.util

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * @ClassName:MyKafkaUtil
 * @author: zhengkw
 * @description:
 * @date: 20/05/18下午 5:31
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object MyKafkaUtil {
  val params = Map[String, String](
    "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
    "group.id" -> "zhengkw",
    // "auto.offset.reset" -> "smallest", // 每次从最早的位置开始读
    "auto.offset.reset" -> "largest", // 每次从最新的位置开始读
    "enable.auto.commit" -> "true" // 自动提交kafka的offset
  )

  def getKafkaStream(ssc: StreamingContext, topic: String, otherTopics: String*) = {
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      params,
      (otherTopics :+ topic).toSet
    ).map(_._2)
  }
}
