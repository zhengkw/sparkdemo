package com.zhengkw.sparkdemo1.acc

/**
 * @ClassName:Test
 * @author: zhengkw
 * @description:
 * @date: 20/05/12下午 11:30
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object Test {
  def main(args: Array[String]): Unit = {

  }
}

/**
 * @descrption: 特质Logger自身类型Exceptio等价于logger父类是Exception
 * @date: 20/05/12 下午 11:33
 * @author: zhengkw
 */
trait Logger {
  this: Exception =>
  def printMsg() = {
    println(this.getMessage)
    println(getMessage)

  }

  //class console extends Logger  //如果要继承Logger必须console也是Exception的子类！

  class console extends Exception with Logger

}