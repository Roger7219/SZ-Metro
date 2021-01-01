package com.ngt.util

import com.ngt.data_processing.SZTAgg
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
 * @create 2020-12-27 0:48
 */
case class Range(station: String, windowEnd: Long, count: Long)

//(station: String, windowEnd: Long, count: Long)
object DrivingRange {

  class RangeCountAgg() extends AggregateFunction[SZTAgg, Long, Long] {
    // 初始值
    override def createAccumulator(): Long = 0L

    override def add(value: SZTAgg, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  class RangeWindowResult() extends WindowFunction[Long, Range, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[Range]): Unit = {
      val itemId = key
      val windowEnd = window.getEnd
      val count = input.iterator.next()
      out.collect(Range(itemId, windowEnd, count))
    }
  }

  class TopNRange(topSize: Int) extends KeyedProcessFunction[Long, Range, String] {

    lazy val passengersCountListState = getRuntimeContext.getListState(new ListStateDescriptor[Range]("itemViewCount-list", classOf[Range]))

    override def processElement(value: Range, ctx: KeyedProcessFunction[Long, Range, String]#Context, out: Collector[String]): Unit = {
      passengersCountListState.add(value)
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, Range, String]#OnTimerContext, out: Collector[String]): Unit = {
      val passengersCounts: ListBuffer[Range] = ListBuffer()
      val iter: util.Iterator[Range] = passengersCountListState.get().iterator()

      while (iter.hasNext) {
        passengersCounts += iter.next()
      }
      passengersCountListState.clear()
      val sortedpassengersCounts: ListBuffer[Range] = passengersCounts.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
      val result: StringBuilder = new StringBuilder

      result.append("=================================================\n")
      // 使用 Timestamp - 1 就是当前窗口的时间
      result.append(new Timestamp(timestamp - 300001)).append(" ---- ").append(new Timestamp(timestamp - 1)).append("\n")
      for (i <- sortedpassengersCounts.indices) {
        val currentItemViewCount = sortedpassengersCounts(i)
        result.append("NO").append(i + 1).append(": \t")
          .append("区间 : ").append(currentItemViewCount.station).append("\t\t")
          .append("客流量 : ").append(currentItemViewCount.count).append("\n")
      }
      result.append("=================================================\n")
      TimeUnit.MICROSECONDS.sleep(100)
      out.collect(result.toString())
    }
  }

}
