package com.zhengkw.realtime.util

import scala.collection.mutable
import scala.util.Random

/**
 * @ClassName:RandomNumUtil
 * @author: zhengkw
 * @description:
 * Date 2020-05-18  14:58
 *
 * 随机生成整数的工具类
 * @date: 20/05/18下午 4:58
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */

object RandomNumUtil {
  val random = new Random()
  /**
   * 返回一个随机的整数 [from, to]
   *
   * @param from
   * @param to
   * @return
   */
  def randomInt(from: Int, to: Int): Int = {
    if (from > to) throw new IllegalArgumentException(s"from = $from 应该小于 to = $to")
    // [0, to - from)  + from [form, to -from + from ]
    random.nextInt(to - from + 1) + from
  }

  /**
   * 随机的Long  [from, to]
   *
   * @param from
   * @param to
   * @return
   */
  def randomLong(from: Long, to: Long): Long = {
    if (from > to) throw new IllegalArgumentException(s"from = $from 应该小于 to = $to")
    random.nextLong().abs % (to - from + 1) + from
  }

  /**
   * 生成一系列的随机值
   *
   * @param from
   * @param to
   * @param count
   * @param canReat 是否允许随机数重复
   */
  def randomMultiInt(from: Int, to: Int, count: Int, canReat: Boolean = true): List[Int] = {
    if (canReat) {
      (1 to count).map(_ => randomInt(from, to)).toList
    } else {
      val set: mutable.Set[Int] = mutable.Set[Int]()
      while (set.size < count) {
        set += randomInt(from, to)
      }
      set.toList
    }
  }


  def main(args: Array[String]): Unit = {
    println(randomMultiInt(1, 15, 10))
    println(randomMultiInt(1, 8, 10, false))
  }
}