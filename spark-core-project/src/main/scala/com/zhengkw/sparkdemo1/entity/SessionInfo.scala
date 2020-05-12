package com.zhengkw.sparkdemo1.entity

case class SessionInfo(sid: String,
                       count: Int) extends Ordered[SessionInfo]{
    // 对咱们业务来说, 千万不要返回0.
    // 如果返回0 在set中会去重
    override def compare(that: SessionInfo): Int = {
        if(this.count > that.count) -1
        else 1
    }
}

/*
Ordered
    让你对象具有与其他的对象排序的功能
Ordering
    比较器, 用来比较两个对象的
 */