package com.ngt.data_processing

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.util
import scala.collection.mutable.ListBuffer

/**
 * @author ngt
 * @create 2020-12-24 14:42
 */

object Income {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputSteam: DataStream[String] = env.readTextFile("ETLdata/metor.csv")

    val dataStream: DataStream[SZT] = inputSteam
      .map(data => {
        val arr: Array[String] = data.split(",")
        SZT(arr(0).toLong, arr(1), arr(2).toDouble, arr(3).toDouble,
          arr(4), arr(5), arr(6), arr(7), arr(8), arr(9))
      })


    val value: DataStream[String] = dataStream
      .keyBy(_.cardId)
      .process(new incomeKeyedProcess)

    value.print()
    env.execute()
  }
}

class incomeKeyedProcess() extends KeyedProcessFunction[String, SZT, String] {
  lazy val listState: ListState[SZT] =
    getRuntimeContext.getListState(new ListStateDescriptor[SZT]("list", classOf[SZT]))

  override def processElement(value: SZT, ctx: KeyedProcessFunction[String, SZT, String]#Context, out: Collector[String]): Unit = {
    val list: ListBuffer[SZT] = ListBuffer()
    listState.add(value)
    val iter: util.Iterator[SZT] = listState.get().iterator()

    var deal_money: Double = 0.0
    var deal_value: Double = 0.0
    while (iter.hasNext) {
      val szt: SZT = iter.next()
      list += szt
      deal_money += szt.deal_money
      deal_value += szt.deal_value
    }
  }
}