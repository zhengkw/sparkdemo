package com.zhengkw.stu.day02.state

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName:StateDemo
 * @author: zhengkw
 * @description: updateStateByKey 使用必须添加ckp
 * @date: 20/05/18上午 10:47
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object StateDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("StateDemo")
    val ssc = new StreamingContext(conf, Seconds(3))
    //添加ckp
    ssc.checkpoint("./ck2")
    //先获取流对象 这里读的是一行！
    val stream = ssc.socketTextStream("hadoop102", 9876)
    stream.flatMap(_.split(" "))
      .map((_, 1))
      .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => {
        // Some(seq.reduce(_ + _) + opt.getOrElse(0))
        Some(seq.sum + opt.getOrElse(0))
      }).print(100)
    ssc.start()
    ssc.awaitTermination()
  }
}
