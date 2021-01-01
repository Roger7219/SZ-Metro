package com.ngt.app

import com.alibaba.fastjson.JSON
import com.ngt.source.MyRedisSourceFun
import com.ngt.util.JsonToCSV
import org.apache.flink.streaming.api.scala._

import java.io.{File, PrintStream}
/**
 * @author ngt
 * @create 2020-12-23 21:42
 */
object RedistoCSV {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val tempFile = "data/log.txt"
    val jsonPath = ""
    val csvPath = ""
    JsonToCSV.jsonToCSV(jsonPath, csvPath)
  }

  def saveTmepFile(env: StreamExecutionEnvironment, outFile: String): Unit = {
    env.setParallelism(8)

    val ymd = "2018-09-01"
    val dataStream: DataStream[String] = env
      .addSource(new MyRedisSourceFun)
      .filter(x => {
        val json = JSON.parseObject(x)
        val deal_date = json.getString("deal_date")
        deal_date.startsWith(ymd)
      })

    dataStream.print()

    val file = new File(outFile)
    if (file.exists()) {
      file.delete()
    }
    val ps = new PrintStream(outFile)
    System.setOut(ps)
    env.execute("saveTmepFile")
  }


  def toJson(env: StreamExecutionEnvironment, inFile: String, outFile: String): Unit = {
    env.setParallelism(8)
    val inputStream: DataStream[String] = env.readTextFile(inFile)

    val dataStream: DataStream[String] = inputStream.map(
      data => data + ","
    )


  }


}


/*
"deal_date": "2018-08-31 22:14:50",
"close_date": "2018-09-01 00:00:00",
"card_no": "CBEHFCFCG",
"deal_value": "0",
"deal_type": "地铁入站",
"company_name": "地铁五号线",
"car_no": "IGT-105",
"station": "布吉",
"conn_mark": "0",
"deal_money": "0",
"equ_no": "263032105"
*/