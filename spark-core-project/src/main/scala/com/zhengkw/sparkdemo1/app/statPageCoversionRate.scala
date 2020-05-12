package com.zhengkw.sparkdemo1.app

import java.text.DecimalFormat

import com.zhengkw.sparkdemo1.entity.UserVisitAction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @ClassName:statPageCoversionRate
 * @author: zhengkw
 * @description:
 * @date: 20/05/12下午 1:50
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object statPageCoversionRate {
  def showConversionRates(sc: SparkContext,
                          userVisitActionRDD: RDD[UserVisitAction],
                          pagesStr: String) = {
    //对pageStr进行转换得到目标
    val pageList = pagesStr.split(",").toList
    val preStr = pageList.init
    val posStr = pageList.tail
    val targeteFlow = preStr.zip(posStr)
      .map({
        case (pre, pos) => s"$pre->$pos"
      })
    //    targeteFlow.foreach(println)

    //计算分母denominator 分母是跳转前页面的访问量，如果是1-2的转换率，则1的访问量作为分母
    //   如果统计1-7的 则只需要算出 1,2。。。6的访问量
    val pageAndCount = userVisitActionRDD.filter(action => pageList.contains(action.page_id.toString))
      //拼接数字聚合算出访问量
      .map({
        case action => (action.page_id, 1)
      }).countByKey()
    //pageAndCount.foreach(println)

    //计算分子
    val actionGroupedRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD
      /*.map(action => (action.session_id, action))
      .groupByKey()*/
      .groupBy(_.session_id)
    val pagesFlowRDD = actionGroupedRDD.flatMap {
      case (sid, actionIt) =>
        // 计算每个session下的跳转量
        // 1->2  2->3   4->5
        // 按照时间进行排序
        val actions = actionIt.toList.sortBy(_.action_time)
        // 计算跳转量   如果能有一个集合存储就是  "1->2",  "2->3", "1->5", ....
        val preActions = actions.init // 干掉最后一个
        val posActions = actions.tail // 干掉第一个
        // 结果中有各种转换流, 我们只需要 1->2 2->3 3->3
        val allFlows = preActions.zip(posActions).map {
          case (pre, post) => s"${pre.page_id}->${post.page_id}"
        }
        // 4.5 过滤出来目标跳转流
        val allTargetFlows: List[String] = allFlows.filter(flow => targeteFlow.contains(flow))
        allTargetFlows
    }
    // 3.2 聚合跳转流
    val pageFlowCount: Array[(String, Int)] = pagesFlowRDD.map((_, 1)).reduceByKey(_ + _).collect

    val f = new DecimalFormat(".00%")
    // 4. 计算跳转率  ("1->2", 1000)   找页面1的点击量 10000     10%
    val result = pageFlowCount.map {
      case (flow, count) =>
        // 4.1 1->2  找到页面1的点击量
        val page = flow.split("->")(0).toLong
        val denominator = pageAndCount(page)
        (flow, f.format(count.toDouble / denominator))
    }

    result.foreach(println)

  }
}
