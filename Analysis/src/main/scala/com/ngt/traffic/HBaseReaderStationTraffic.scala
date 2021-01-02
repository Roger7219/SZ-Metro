package com.ngt.traffic

import com.ngt.util.HBaseUtil.HBaseReader
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 * @author ngt
 * @create 2021-01-03 3:05
 */
case class Traffic(time: String, rank: String, station: String, count: String)

object HBaseReaderStationTraffic {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    /*
      场景1:查询 2018-09-01 08:30 - 2018-09-01 08:45 各站点最近五分钟的客流
     */
    // 注意 区间是左闭右开
    val dataStream1: DataStream[(String, String)] =
    env.addSource(new HBaseReader("StationTraffic", "traffic",
      "2018-09-01 08:30", "2018-09-01 08:46"))

    dataStream1.map(x => {
      val keys: Array[String] = x._1.split(" ")
      val values: Array[String] = x._2.split("_")
      Traffic("时间:" + keys(1),  "排名:" + keys(2), "站点:" + values(1),"客流量:" + values(0))
    })
      .map(data => {
        println(data.time, data.rank, data.station, data.count)
      })



    /*
      场景2:查询 2018-09-01 06:30 - 2018-09-01 11:30 客流量排名前 3 的站点
     */

    val dataStream2: DataStream[(String, String)] =
      env.addSource(new HBaseReader("StationTraffic", "traffic",
        "2018-09-01 06:30", "2018-09-01 11:31"))

    dataStream2.map(x => {
      val keys: Array[String] = x._1.split(" ")
      val values: Array[String] = x._2.split("_")
      Traffic("时间:" + keys(1), "排名:" + keys(2), "站点:" + values(1), "客流量:" + values(0))
    })
      .filter(_.rank.substring(3).toInt <= 3)
      .map(data => {
        println(data.time, data.rank, data.station, data.count)
      })

    env.execute()
  }
}
