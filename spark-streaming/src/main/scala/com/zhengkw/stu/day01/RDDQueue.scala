package com.zhengkw.stu.day01

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @ClassName:RDDQueue
 * @author: zhengkw
 * @description:
 * @date: 20/05/16上午 11:36
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object RDDQueue {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("RDDQueue")
    val ssc = new StreamingContext(conf, Seconds(3))
    //获取sc
    val sc = ssc.sparkContext
    //创建一个队列
    val queue = mutable.Queue[RDD[Int]]()

    val rddDS = ssc.queueStream(queue, false)
    val value = rddDS.reduce(_ + _)
    value.print
    ssc.start()

    //循环添加队列
    while (true) {
      println(queue.size)
      val rdd = ssc.sparkContext.parallelize(1 to 100)
      queue.enqueue(rdd)
      Thread.sleep(100)
    }
    ssc.awaitTermination()
  }
}