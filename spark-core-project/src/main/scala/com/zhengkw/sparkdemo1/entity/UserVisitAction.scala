package com.zhengkw.sparkdemo1.entity

/**
 * @ClassName:UserVisitAction
 * @author: zhengkw
 * @description:
 * @date: 20/05/11下午 2:07
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
case class UserVisitAction(
                            date: String,
                            user_id: Long,
                            session_id: String,
                            page_id: Long,
                            action_time: String,
                            search_keyword: String,
                            click_category_id: Long,
                            click_product_id: Long,
                            order_category_ids: String, // "1,10,30"
                            order_product_ids: String, // "10,11"
                            pay_category_ids: String,
                            pay_product_ids: String,
                            city_id: Long
                          )


