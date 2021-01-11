package com.ngt.traffic

import com.ngt.util.HBaseUtil.HBaseReader
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 * @author ngt
 * @create 2021-01-03 14:04
 */

case class DrivingRange(time: String, rank: String, range: String, count: String)

object HBaseReaderDrivingRange {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataStream2: DataStream[(String, String)] =
      env.addSource(new HBaseReader("DrivingRange", "traffic",
        "2018-09-01 06:30", "2018-09-01 11:31"))

    dataStream2.map(x => {
      val keys: Array[String] = x._1.split(" ")
      val values: Array[String] = x._2.split("_")
      DrivingRange("时间:" + keys(1), "排名:" + keys(2), "乘车区间:" + values(1), "客流量:" + values(0))
    })
      .filter(_.rank.substring(3).toInt <= 3)
      .map(data => {
        println(data.time, data.rank, data.range, data.count)
      })
    env.execute()
  }
}
