package com.ngt.app


import org.apache.flink.streaming.api.scala._

import java.io.File
import java.text.SimpleDateFormat

/**
 * @author ngt
 * @create 2020-12-23 22:18
 */

/*
  原始数据, 丢弃 close_date 去除 deal_date 的引号并转为时间戳样式
  0 "deal_date": "2018-08-31 22:14:50",
  1 "close_date": "2018-09-01 00:00:00",
  2 "card_no": "CBEHFCFCG",
  3 "deal_value": "0",   交易值  需要除以 100.0
  4 "deal_type": "地铁入站",
  5 "company_name": "地铁五号线",
  6 "car_no": "IGT-105",
  7 "station": "布吉",
  8 "conn_mark": "0",  联程标记
  9 "deal_money": "0",  优惠后金额  需要除以 100.0
  10 "equ_no": "263032105" 设备编码

 */


/*

 */

/*

 */
// 时间 卡号 交易金额 实际交易金额 交易类型  线路 是否连乘 站点 车牌 设备编码
case class SZT(time: Long, cardId: String, deal_value: Double, deal_money: Double, deal_type: String,
               lineName: String, conn_mark: String, station: String, carId: String, deviceId: String)

object DataClassification {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputString: DataStream[String] = env.readTextFile("data/szt.csv")

    val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var time: Long = 0L

    val metorDataStream: DataStream[SZT] = inputString
      .filter(data => {
        data.contains("2018-09-01 00:00:00")
      })
      .map(data => {
        val arr: Array[String] = data.split(",")
        time = format.parse(arr(0).substring(1, arr(0).length - 1)) getTime()
        SZT(time, arr(2), arr(3).toInt/100.0, arr(9).toInt/100.0,
          arr(4),arr(5), arr(8), arr(7),arr(6), arr(10))
      })
      .filter(data=>
      data.deal_type!="巴士")


    val busDataStream: DataStream[SZT] = inputString
      .filter(data => {
        data.contains("2018-09-01 00:00:00")
      })
      .map(data => {
        val arr: Array[String] = data.split(",")
        time = format.parse(arr(0).substring(1, arr(0).length - 1)) getTime()
        SZT(time, arr(2), arr(3).toInt/100.0, arr(9).toInt/100.0,
          arr(4),arr(5), arr(8), arr(7),arr(6), arr(10))
      })
      .filter(data=>
        data.deal_type =="巴士")

    val metorPath = "ETLdata/metor.csv"
    val busPath = "ETLdata/bus.csv"

    val metorFile = new File(metorPath)
    if (metorFile.exists()) metorFile.delete()

    val busFile = new File(busPath)
    if(busFile.exists()) busFile.delete()

    metorDataStream.writeAsCsv(metorPath)
    busDataStream.writeAsCsv(busPath)
    env.execute()
  }
}

/*
原始数据, 丢弃 close_date 去除 deal_date 的引号并转为时间戳样式
0 "deal_date": "2018-08-31 22:14:50",
1 "close_date": "2018-09-01 00:00:00",
2 "card_no": "CBEHFCFCG",
3 "deal_value": "0",   交易值  需要除以 100.0
4 "deal_type": "地铁入站",
5 "company_name": "地铁五号线",
6 "car_no": "IGT-105",
7 "station": "布吉",
8 "conn_mark": "0",  联程标记
9 "deal_money": "0",  优惠后金额  需要除以 100.0
10 "equ_no": "263032105" 设备编码

Sep 1, 2018 @ 05:30:00.000  Sep 1, 2018 @ 12:00:00.000
 */
