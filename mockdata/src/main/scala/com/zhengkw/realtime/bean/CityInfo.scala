package com.zhengkw.realtime.bean

/**
 * @ClassName:CityInfo
 * @author: zhengkw
 * @date: 20/05/18下午 5:15
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 * @description:城市表
 * @param city_id   城市 id
 * @param city_name 城市名
 * @param area      城市区域
 */
case class CityInfo(city_id: Long,
                    city_name: String,
                    area: String)

