package com.ngt.app

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
/**
 * @author ngt
 * @create 2020-12-23 21:25
 *         读取 jsons，存入 redis
 */
object JsonToRedis {
  val JSON_PATH = "data/szt-data-page.jsons"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(8)

    val dataStream: DataStream[JSONObject] = env.readTextFile(JSON_PATH)
      .filter(_.nonEmpty)
      .map(x => {
        JSON.parseObject(x)
      })


    //定义 redis 参数
    val jedis = new FlinkJedisPoolConfig.Builder().setHost("192.168.31.8").build()

    dataStream.addSink(new RedisSink(jedis, new MyRedisSinkFun))
    env.execute("JsonsToRedis")
  }
}

case class MyRedisSinkFun() extends RedisMapper[JSONObject] {
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "szm:json")
  }

  override def getKeyFromData(data: JSONObject): String = data.getIntValue("page").toString

  override def getValueFromData(data: JSONObject): String = data.toString
}
