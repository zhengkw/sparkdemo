package com.zhengkw.exc.acc

import com.zhengkw.exc.entity.UserVisitAction
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable


/**
 * @ClassName:MyAccumulator
 * @author: zhengkw
 * @description:
 * @date: 20/05/13下午 9:55
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
class MyAccumulator() extends AccumulatorV2[UserVisitAction,
  mutable.Map[Long, (Long, Long, Long)]] {
  private val map: mutable.Map[Long, (Long, Long, Long)] = mutable.Map[Long, (Long, Long, Long)]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[Long, (Long, Long, Long)]] = {
    val acc = new MyAccumulator
    acc.map.synchronized {
      acc.map ++= this.map
    }
    acc
  }

  override def reset(): Unit = this.map.clear()

  override def add(v: UserVisitAction): Unit = {
    v match {
      case () =>
      case () =>
      case _ =>
    }

  }

  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[Long, (Long, Long, Long)]]): Unit = ???

  override def value: mutable.Map[Long, (Long, Long, Long)] = ???
}


