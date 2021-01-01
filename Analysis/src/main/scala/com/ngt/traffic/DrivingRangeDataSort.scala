package com.ngt.traffic

import com.ngt.data_processing.SZTAgg
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.io.File
import java.time.Duration
import scala.collection.mutable.ListBuffer

/**
 * @author ngt
 * @create 2020-12-27 0:19
 */

/*
case class SZTAgg(
               cardId: String,
               inTime: Long, outTime: Long, time: Int, var sumTime: Int, deal_value: Double, deal_money: Double,
               inLine: String, outLine: String, lineSection: String, acrossLine: Int,
               inStation: String, outStation: String, stationSection: String, conn_mark: String,
               inCar: String, outCar: String, inDevice: String, outDevice: String, deviceSection: String
             )

*/

object DrivingRangeDataSort {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputSteam: DataStream[String] = env.readTextFile("ETLdata/metor_join.csv")

    val dataStream: DataStream[SZTAgg] = inputSteam
      .map(data => {
        val r: Array[String] = data.split(",")
        SZTAgg(r(0), r(1).toLong, r(2).toLong, r(3).toInt, r(4).toInt, r(5).toDouble, r(6).toDouble,
          r(7), r(8), r(9), r(10).toInt,
          r(11), r(12), r(13), r(14),
          r(15), r(16), r(17), r(18), r(19))
      })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofHours(6))
        .withTimestampAssigner(new SerializableTimestampAssigner[SZTAgg] {
          override def extractTimestamp(element: SZTAgg, recordTimestamp: Long): Long = element.outTime
        }))

    val value: DataStream[SZTAgg] = dataStream
      .timeWindowAll(Time.hours(6))
      .process(new DrivingRangeFunc)

    val filePath = "TrafficData/DrivingRangeDataSort.csv"
    val file: File = new File(filePath)
    if (file.exists()) file.delete()
    value.writeAsCsv(filePath)

    env.execute()
  }
}

class DrivingRangeFunc() extends ProcessAllWindowFunction[SZTAgg, SZTAgg, TimeWindow] {
  override def process(context: Context, elements: Iterable[SZTAgg], out: Collector[SZTAgg]): Unit = {
    val inList: ListBuffer[SZTAgg] = new ListBuffer[SZTAgg]()
    for (r <- elements) {
      inList += r
    }

    val sortList: ListBuffer[SZTAgg] = inList.sortBy(_.outTime)(Ordering.Long)

    for (i <- sortList) {
      out.collect(i)
    }
  }
}
