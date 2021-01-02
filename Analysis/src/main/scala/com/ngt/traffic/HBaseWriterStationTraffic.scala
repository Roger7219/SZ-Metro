package com.ngt.traffic

import com.ngt.data_processing.SZT
import com.ngt.util.HBaseUtil.{HBaseWriter}
import com.ngt.util.StationTop
import com.ngt.util.TopTraffic.{StationWindowResult, TopNStationHbase, TrafficCountAgg}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import java.util.Properties

/**
 * @author ngt
 * @create 2020-12-26 22:25
 * 将站点客流信息写入 Hbase
 */


object HBaseWriterStationTraffic {

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
      // 如果只需要查询出站客流，启用此过滤条件，入站同理
//      .filter(_.deal_type == "地铁出站")
      .keyBy(_.station)
      // 统计最近 5 分钟内的客流， 每 1分钟滚动一次
      .timeWindow(Time.minutes(5), Time.minutes(1))
      .aggregate(new TrafficCountAgg, new StationWindowResult)

    val resultStream: DataStream[String] = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNStationHbase)

    /*
      将实时客流数据写入 Hbase
      在 Hbase shell 中使用命令 ：create 'StationTraffic', {NAME => 'traffic'}  创建表
     */
    resultStream.addSink(new HBaseWriter("StationTraffic","traffic"))
    env.execute()
  }
}

