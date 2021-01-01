package com.ngt.data_processing

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.io.File
import java.time.Duration
import scala.collection.mutable.ListBuffer

/**
 * @author ngt
 * @create 2020-12-24 2:33
 */

case class SZT(time: Long, cardId: String, deal_value: Double, deal_money: Double, deal_type: String,
               lineName: String, conn_mark: String, station: String, carId: String, deviceId: String)


object DataFilter {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputSteam: DataStream[String] = env.readTextFile("ETLdata/metor.csv")

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
      .keyBy(_.cardId)
      .timeWindow(Time.hours(6))
      .process(new func)

    //    value.print()
    val filePath = "ETLdata/metor_filter.csv"
    val file: File = new File(filePath)
    if (file.exists()) file.delete()

    //    val ps = new PrintStream("ETLdata/log.csv")
    //    System.setOut(ps)
    value.writeAsCsv(filePath)
    env.execute()

  }
}


class func() extends ProcessWindowFunction[SZT, SZT, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[SZT], out: Collector[SZT]): Unit = {
    // 接收分组的输入
    val inList: ListBuffer[SZT] = new ListBuffer[SZT]()
    for (r <- elements) {
      inList += r
    }

    // 数据按照时间戳升序
    val sortList: ListBuffer[SZT] = inList.sortBy(_.time)(Ordering.Long)

    // 过滤1：按照时间排序之后删除，开头为地铁出站的信息，删除最后一条为地铁入站的信息
    if (sortList(0).deal_type == "地铁出站") {
      sortList.remove(0)
    } else if (sortList.last.deal_type == "地铁入站") {
      sortList.remove(sortList.length - 1)
    }

    val outList: ListBuffer[SZT] = new ListBuffer()
    // 过滤2：丢弃数据条数等于 1 条的
    // 过滤3 将按照时间排序之后出现 连续两条都是入站或出站的数据,
    // 对于正确的数据 偶数角标必然是入站 奇数角标必然是出站
    val len: Int = sortList.size
    if (len > 1) {
      for (i <- Range(0, len, 2)) {
        if (sortList(i).deal_type == "地铁入站" && i + 1 < len
          && sortList(i + 1).deal_type == "地铁出站" &&
          sortList(i).station != sortList(i + 1).station) {
          outList.append(sortList(i))
          outList.append(sortList(i + 1))
        }
      }
    }

    if(outList.size >=2){
      for (i <- outList) {
        out.collect(i)
      }
    }
  }
}


/*
    //      for (i <- 0 until sortList.size) {
    //        if (i == 0) {
    //          if (sortList(0).deal_type == "地铁入站") {
    //            outList.append(sortList(0))
    //          }
    //        } else if (i == (sortList.size - 1)) {
    //          if (sortList(sortList.size - 1).deal_type == "地铁出站") {
    //            outList.append(sortList(sortList.size - 1))
    //          }
    //        } else {
    //          outList.append(sortList(i))
    //        }
    //      }


    //
    //
    //    val flag:Boolean

    // 标识数据是否合法
    //    var flag: Boolean = true
//    if (outList.size >= 2) {
//      breakable {
//        for (i <- outList.indices) {
//          // 出现偶数角标为出站 或  出现奇数角标为入站   就清空 outList
//          if (((i & 1) == 0 && outList(i).deal_type == "地铁出站") ||
//            ((i & 1) == 1 && outList(i).deal_type == "地铁入站")) {
//            outList.clear()
//            break
//            //          flag = false
//          }
//        }
//      }
//    }

 */
