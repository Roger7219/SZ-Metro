package com.ngt.traffic

import com.ngt.data_processing.SZTAgg
import com.ngt.util.DrivingRange.{RangeCountAgg, RangeWindowResult, TopNRange}
import com.ngt.util.Range
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author ngt
 * @create 2020-12-27 0:15
 */
object TopDrivingRange {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputSteam: DataStream[String] = env.readTextFile("TrafficData/DrivingRangeDataSort.csv")


    val dataStream: DataStream[SZTAgg] = inputSteam
      .map(data => {
        val arr: Array[String] = data.split(",")
        val r: Array[String] = data.split(",")
        SZTAgg(r(0), r(1).toLong, r(2).toLong, r(3).toInt, r(4).toInt, r(5).toDouble, r(6).toDouble,
          r(7), r(8), r(9), r(10).toInt,
          r(11), r(12), r(13), r(14),
          r(15), r(16), r(17), r(18), r(19))
      })
      .assignAscendingTimestamps(_.outTime)

    val aggStream: DataStream[Range] = dataStream
      .keyBy(_.stationSection)
      .timeWindow(Time.minutes(5), Time.minutes(1))
      .aggregate(new RangeCountAgg, new RangeWindowResult)

    val resultStream: DataStream[String] = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNRange(10))

    resultStream.print()
    env.execute()
  }
}
