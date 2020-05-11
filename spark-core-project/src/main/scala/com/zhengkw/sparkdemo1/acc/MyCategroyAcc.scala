package com.zhengkw.sparkdemo1.acc

import com.zhengkw.sparkdemo1.entity.UserVisitAction
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 * @ClassName:MyCategroyAcc
 * @author: zhengkw
 * @description:
 * 累加器类型:  in UserVisitAction  out Map
 * 最终的返回值:
 * 计算每个品类的 点击量, 下单量, 支付量
 * Map[品类, (clickCount, orderCount, payCount)]
 * @date: 20/05/11下午 2:17
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
class MyCategroyAcc() extends AccumulatorV2[UserVisitAction,
  mutable.Map[String, (Long, Long, Long)]] {
  private val _map: mutable.Map[String, (Long, Long, Long)] = mutable.Map[String, (Long, Long, Long)]()

  override def isZero: Boolean = _map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[String, (Long, Long, Long)]] = {
    //获得累加器对象
    val acc = new MyCategroyAcc
    //加锁
    acc._map.synchronized {
      acc._map ++= this._map
    }
    acc
  }

  override def reset(): Unit = _map.clear()

  // 重置累加器
  override def add(v: UserVisitAction): Unit =
    v match {
      //点击量, 下单量, 支付量
      case action if v.click_category_id != -1 =>
        // 点击的品类id
        val cid = action.click_category_id.toString
        // map中已经存储的cid的(点击量,下单量,支付量)
        val (click, order, pay) = _map.getOrElse(cid, (0L, 0L, 0L))
        _map += cid -> (click + 1L, order, pay)

      case action if v.order_category_ids != "null" =>
        // "1,2,3"
        val cids: Array[String] = action.order_category_ids.split(",")
        cids.foreach(cid => {
          val (click, order, pay) = _map.getOrElse(cid, (0L, 0L, 0L))
          _map += cid -> (click, order + 1L, pay)
        })
      case action if v.pay_category_ids != "null" =>
        val cids: Array[String] = action.pay_category_ids.split(",")
        cids.foreach(cid => {
          val (click, order, pay) = _map.getOrElse(cid, (0L, 0L, 0L))
          _map += cid -> (click, order, pay + 1L)
        })
      case _ =>
    }

  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[String, (Long, Long, Long)]]): Unit =

    other match {
      case o: MyCategroyAcc =>
        o._map.foldLeft(this._map) {
          case (map, (cid, (click, order, pay))) =>
            val (thisClick, thisOrder, thisPay) = map.getOrElse(cid, (0L, 0L, 0L))
            map += cid -> (thisClick + click, thisOrder + order, thisPay + pay)
            map
        }

      case _ =>
        throw new UnsupportedOperationException
    }


  override def value: mutable.Map[String, (Long, Long, Long)] = _map
}
