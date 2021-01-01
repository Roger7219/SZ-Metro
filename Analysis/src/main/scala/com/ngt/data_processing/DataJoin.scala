package com.ngt.data_processing

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.io.File
import scala.collection.mutable.ListBuffer

/**
 * @author ngt
 * @create 2020-12-25 23:53
 */
/*
 SZT(time: Long, cardId: String, deal_value: Double, deal_money: Double, deal_type: String,
               lineName: String, conn_mark: String, station: String, carId: String, deviceId: String)

 cardId
 入站时间，出站时间， 单次乘车时间，      总乘车时间， 优惠前车费，优惠后车费
 入站线路，出站线路， 入站线路-出站线路，  是否跨线路
 入站站点，出站站点， 入站站点-出站站点，  是否连乘
 入站车牌，出站车牌， 入站闸机，          出站闸机， 入站闸机-出站闸机。

startTime
 总乘车时间是单次乘车时间之和，因为有些乘客可能多次乘车
*/
case class SZTAgg(
                   cardId: String,
                   inTime: Long, outTime: Long, time: Int, var sumTime: Int, deal_value: Double, deal_money: Double,
                   inLine: String, outLine: String, lineSection: String, acrossLine: Int,
                   inStation: String, outStation: String, stationSection: String, conn_mark: String,
                   inCar: String, outCar: String, inDevice: String, outDevice: String, deviceSection: String
                 )


//case class SZTAgg(
//                   cardId: String,
//                   inTime: Long, time: Int, var sumTime: Int, deal_value: Double, deal_money: Double,
//                   inLine: String, outLine: String, lineSection: String, acrossLine: Int,
//                   inStation: String, outStation: String, stationSection: String, conn_mark: String,
//                   inCar: String, outCar: String, inDevice: String, outDevice: String, deviceSection: String
//                 )

object DataJoin {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputSteam: DataStream[String] = env.readTextFile("ETLdata/metor_filter.csv")

    val dataStream: DataStream[SZT] = inputSteam
      .map(data => {
        val arr: Array[String] = data.split(",")
        SZT(arr(0).toLong, arr(1), arr(2).toDouble, arr(3).toDouble,
          arr(4), arr(5), arr(6), arr(7), arr(8), arr(9))
      })
      //      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofHours(6))
      //      .withTimestampAssigner(new SerializableTimestampAssigner[SZT] {
      //        override def extractTimestamp(element: SZT, recordTimestamp: Long): Long = element.time
      //      }))
      .assignAscendingTimestamps(_.time)

    val value: DataStream[SZTAgg] = dataStream
      .keyBy(_.cardId)
      .timeWindow(Time.hours(5))
      .process(new dataAggregation)

    val filePath = "ETLdata/metor_join.csv"
    val file: File = new File(filePath)
    if (file.exists()) file.delete()

    value.writeAsCsv(filePath)
    env.execute()
  }
}

class dataAggregation() extends ProcessWindowFunction[SZT, SZTAgg, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[SZT], out: Collector[SZTAgg]): Unit = {
    val inList: ListBuffer[SZT] = ListBuffer()
    for (r <- elements) {
      inList.append(r)
    }

    val outList: ListBuffer[SZTAgg] = ListBuffer()
    // 数据按照时间戳升序, 可能不需要，以防万一
    val sortList: ListBuffer[SZT] = inList.sortBy(_.time)

    val len: Int = sortList.size
    // 总共乘车时间
    var sumTime: Int = 0

    for (i <- Range(0, len, 2)) {

      val inSZT: SZT = sortList(i)
      val outSZT: SZT = sortList(i + 1)
      if(i>4) println(inSZT.cardId)
      // 判断是否跨线路乘车
      var acrossLine: Int = 0
      if (inSZT.lineName != outSZT.lineName) acrossLine = 1

      // 单次乘车时间
      var time: Int = ((outSZT.time - inSZT.time) / 1000).toInt
      sumTime += time
      // 进站出站相同的视为不合法数据，不予聚合
      if (inSZT.station != outSZT.station) {
        val agg: SZTAgg = SZTAgg(inSZT.cardId, inSZT.time, outSZT.time, time, 0, outSZT.deal_value, outSZT.deal_money,
          inSZT.lineName.substring(2), outSZT.lineName.substring(2),
          inSZT.lineName.substring(2) + "->" + outSZT.lineName.substring(2), acrossLine,
          inSZT.station, outSZT.station, inSZT.station + "->" + outSZT.station, outSZT.conn_mark,
          inSZT.carId, outSZT.carId, inSZT.deviceId, outSZT.deviceId, inSZT.deviceId + "->" + outSZT.deviceId)

        outList.append(agg)
      }

    }

    for (i <- outList) {
      i.sumTime = sumTime
      out.collect(i)
    }

  }
}

/*
case class SZTAgg(inTime: Long, outTime: Long, time: Int, sumTime: Int, deal_value: Double, deal_money: Double,
                  inLine: String, outLine: String, lineSection: String, acrossLine: Int,
                  inStation: String, outStation: String, stationSection: String, conn_mark: String,
                  inCar: String, outCar: String, inDevice: String, outDevice: String, deviceSection: String
                 )
 */