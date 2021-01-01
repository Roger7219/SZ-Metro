package com.ngt.util

import com.ngt.data_processing.SZT
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp
import java.util
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer

/**
 * @author ngt
 * @create 2020-12-26 22:55
 */
case class StationTop(station: String, windowEnd: Long, count: Long)

object TopTraffic {

  class TrafficCountAgg() extends AggregateFunction[SZT, Long, Long] {
    // 初始值
    override def createAccumulator(): Long = 0L

    override def add(value: SZT, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  class StationWindowResult() extends WindowFunction[Long, StationTop, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[StationTop]): Unit = {
      val itemId = key
      val windowEnd = window.getEnd
      val count = input.iterator.next()
      out.collect(StationTop(itemId, windowEnd, count))
    }
  }

  class TopNStation(topSize: Int, inout: String) extends KeyedProcessFunction[Long, StationTop, String] {
    lazy val passengersCountListState = getRuntimeContext.getListState(new ListStateDescriptor[StationTop]("itemViewCount-list", classOf[StationTop]))

    override def processElement(value: StationTop, ctx: KeyedProcessFunction[Long, StationTop, String]#Context, out: Collector[String]): Unit = {
      passengersCountListState.add(value)
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, StationTop, String]#OnTimerContext, out: Collector[String]): Unit = {
      val passengersCounts: ListBuffer[StationTop] = ListBuffer()
      val iter: util.Iterator[StationTop] = passengersCountListState.get().iterator()

      while (iter.hasNext) {
        passengersCounts += iter.next()
      }
      passengersCountListState.clear()

      val sortedpassengersCounts: ListBuffer[StationTop] = passengersCounts.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
      val result: StringBuilder = new StringBuilder

      result.append("=================================================\n")
      // 使用 Timestamp - 1 就是当前窗口的时间
      result.append(new Timestamp(timestamp - 300001)).append(" ---- ").append(new Timestamp(timestamp - 1)).append("\n")
      for (i <- sortedpassengersCounts.indices) {
        val currentItemViewCount = sortedpassengersCounts(i)
        result.append("NO").append(i + 1).append(": \t")
          .append("车站 : ").append(currentItemViewCount.station).append("\t\t\t")
          .append(inout).append(" : ").append(currentItemViewCount.count).append("\n")
      }
      result.append("=================================================\n")
      TimeUnit.MICROSECONDS.sleep(100)
      out.collect(result.toString())
    }
  }

}
