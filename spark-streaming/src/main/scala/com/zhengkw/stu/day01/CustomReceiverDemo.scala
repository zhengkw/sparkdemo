package com.zhengkw.stu.day01

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

/**
 * @ClassName:CustomReceiverDemo
 * @author: zhengkw
 * @description:
 * @date: 20/05/16下午 2:40
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object CustomReceiverDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomReceiverDemo").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(3))
    val rids = ssc.receiverStream(new MyReceiver("hadoop102", 10000))
    rids.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()
    //启动！
    ssc.start()
    ssc.awaitTermination()
  }
}

class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  var socket: Socket = _
  var reader: BufferedReader = _

  override def onStart(): Unit = {
    socket = new Socket(host, port)
    try {
      val is = socket.getInputStream
      val isr = new InputStreamReader(is, "utf8")
      val bf = new BufferedReader(isr)
      var line = bf.readLine()
      while (line != null) {
        store(line)
        line = bf.readLine()
      }
    } catch {
      case exception: Exception =>
    } finally {
      restart("重启") // 先回调onStop, 再回调 onStart
    }


  }

  /**
   * @descrption: 释放资源
   * @return: void
   * @date: 20/05/17 下午 9:04
   * @author: zhengkw
   */
  override def onStop(): Unit = {
    if (reader != null) reader.close()
    if (socket != null) socket.close()
  }

  def RunInThread(op: => Unit) = {
    new Thread(
      new Runnable {
        override def run(): Unit = op
      }
    ).start()
  }

}