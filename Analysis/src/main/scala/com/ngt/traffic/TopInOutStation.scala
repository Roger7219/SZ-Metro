package com.ngt.traffic

import com.ngt.data_processing.SZT
import com.ngt.util.StationTop
import com.ngt.util.TopTraffic.{StationWindowResult, TopNStation, TrafficCountAgg}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import java.util.Properties

/**
 * @author ngt
 * @create 2020-12-26 22:25
 *         滚动统计不同站点，线路 出站，入站的客流量
 */


object TopInOutStation {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream: DataStream[String] = env.readTextFile("TrafficData/metor_time_sort.csv")

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    properties.setProperty("group.id", "consumer-group")

    // 使用通配符 同时匹配多个Kafka主题
    //    val inputStream = env.addSource(new FlinkKafkaConsumer011[String](java.util.regex.Pattern.compile("sz-metor-traffic[0-9]]"), new SimpleStringSchema(), properties))



    val dataStream: DataStream[SZT] = inputStream
      .map(data => {
        val arr: Array[String] = data.split(",")
        SZT(arr(0).toLong, arr(1), arr(2).toDouble, arr(3).toDouble,
          arr(4), arr(5), arr(6), arr(7), arr(8), arr(9))
      })
      .assignAscendingTimestamps(_.time)

    val aggStream: DataStream[StationTop] = dataStream
//      .filter(_.deal_type == "地铁出站")
      .keyBy(_.station)
      .timeWindow(Time.minutes(5), Time.minutes(1))
      .aggregate(new TrafficCountAgg, new StationWindowResult)

    val resultStream: DataStream[String] = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNStation(20, "总客流"))

    resultStream.print()
    env.execute()

  }
}
