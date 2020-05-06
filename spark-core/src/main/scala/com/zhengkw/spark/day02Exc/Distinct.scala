package com.zhengkw.spark.day02Exc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:Distinct
 * @author: zhengkw
 * @description:
 * @date: 20/05/06上午 8:27
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object Distinct {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Distinct").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    /*val list1 = List(30, 50, 70, 60, 10, 20, 60, 10, 20, 60, 10, 2, 10, 20, 60, 10, 20)
    val rdd1: RDD[Int] = sc.parallelize(list1, 2)

    val rdd2: RDD[Int] = rdd1.distinct()  // 去重
    println(rdd2.collect().mkString(", "))*/

    val users = User(10, "zs") :: User(20, "lisi") :: User(10, "abc") :: Nil
    val rdd1 = sc.parallelize(users, 2)
  }

  case class User(age: Int, name: String) {
    override def hashCode(): Int = this.age

    override def equals(obj: Any): Boolean = {
      obj match {
        case null => false
        case user: User => user.age == this.age
        case _ => false
      }
    }
  }


}
