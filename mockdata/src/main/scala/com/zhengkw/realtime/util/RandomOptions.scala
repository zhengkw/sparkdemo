package com.zhengkw.realtime.util

import scala.collection.mutable.ListBuffer

/**
 * @ClassName:RandomOptions
 * @author: zhengkw
 * @description:
 * @date: 20/05/18下午 5:00
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object RandomOptions {
  def apply[T](opts: (T, Int)*): RandomOptions[T] = {
    val randomOptions = new RandomOptions[T]()
    randomOptions.totalWeight = (0 /: opts) (_ + _._2) // 计算出来总的比重
    opts.foreach {
      case (value, weight) => randomOptions.options ++= (1 to weight).map(_ => value)
    }
    randomOptions
  }


  def main(args: Array[String]): Unit = {
    // 测试
    val opts = RandomOptions(("张三", 10), ("李四", 30), ("ww", 20))


    for (i <- 1 to 10) println(opts.getRandomOption())

  }
}

// 工程师 10  程序猿 10  老师 20
class RandomOptions[T] {
  var totalWeight: Int = _
  var options = ListBuffer[T]()

  /**
   * 获取随机的 Option 的值
   *
   * @return
   */
  def getRandomOption() = {
    options(RandomNumUtil.randomInt(0, totalWeight - 1))
  }

}
