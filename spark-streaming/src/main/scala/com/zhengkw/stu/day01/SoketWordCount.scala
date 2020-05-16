package com.zhengkw.stu.day01

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:SoketWordCount
 * @author: zhengkw
 * @description:
 * @date: 20/05/16上午 10:40
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object SoketWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SoketWordCount").setMaster("local[2]")
    //获取上下文
    val ssc = new StreamingContext(conf, Seconds(3))

    val sourceStream = ssc.socketTextStream("hadoop102", 9999)
    // 2. 从数据源读取数据, 得到 DStream  (RDD, DataSet, DataFrame)
    val resultStream = sourceStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //打印前N个
    resultStream.print(100)
    // 4. 启动流
    ssc.start()
    // 5. 阻止程序退出
    ssc.awaitTermination()

  }
}
