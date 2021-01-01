package com.ngt.traffic

import com.ngt.data_processing.SZT
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
 * @create 2020-12-26 22:26
 */

object DataSorting {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputSteam: DataStream[String] = env.readTextFile("ETLdata/metor_join.csv")

    val dataStream: DataStream[SZT] = inputSteam
      .map(data => {
        val arr: Array[String] = data.split(",")
        SZT(arr(0).toLong, arr(1), arr(2).toDouble, arr(3).toDouble,
          arr(4), arr(5), arr(6), arr(7), arr(8), arr(9))
      })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofHours(6))
        .withTimestampAssigner(new SerializableTimestampAssigner[SZT] {
          override def extractTimestamp(element: SZT, recordTimestamp: Long): Long = element.time
        }))

    val value: DataStream[SZT] = dataStream
      // 地铁运营时间肯定是 6 点以后， 6点之前的数据多是内部人员的数据视为无效数据
      .filter(_.time >= 1535752800000L)
      .timeWindowAll(Time.hours(6))
      .process(new DataSortingFunc)
    //    value.print()

    val filePath = "TrafficData/metor_time_sort.csv"
    val file: File = new File(filePath)
    if (file.exists()) file.delete()
    value.writeAsCsv(filePath)

    env.execute()
  }
}

class DataSortingFunc() extends ProcessAllWindowFunction[SZT, SZT, TimeWindow] {
  override def process(context: Context, elements: Iterable[SZT], out: Collector[SZT]): Unit = {
    val inList: ListBuffer[SZT] = new ListBuffer[SZT]()
    for (r <- elements) {
      inList += r
    }

    val sortList: ListBuffer[SZT] = inList.sortBy(_.time)(Ordering.Long)

    for (i <- sortList) {
      out.collect(i)
    }
  }
}
