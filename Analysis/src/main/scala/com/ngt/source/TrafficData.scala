package com.ngt.source

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.io.BufferedSource

/**
 * @author ngt
 * @create 2020-12-26 23:49
 */
object TrafficData {

  def main(args: Array[String]): Unit = {
    writeToKafka("sz-metor-traffic")
  }
  def writeToKafka(topic: String): Unit = {

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)
    val source: BufferedSource = io.Source.fromFile("TrafficData/metor_time_sort.csv")

    for (line <- source.getLines()) {
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topic, line)
      producer.send(record)
      TimeUnit.MILLISECONDS.sleep(100)
    }
    producer.close()
  }
}
