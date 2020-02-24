package com.atguigu.hotitem

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducerTest {

  def writeToKafka(topic: String): Unit = {
    //定义配置参数
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    //因为是生产者，所以需要序列化数据
    properties.setProperty("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")

    //创建一个kafka producer
    val producer = new KafkaProducer[String, String](properties)
    // 从文件中读取测试数据，逐条发送
    val bufferedSource = io.Source.fromFile("D:\\workspace\\IdeaProject\\UserBehaviorAnalysis\\HottemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    for (line <- bufferedSource.getLines()) {
      //读取到一行发送一行
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
    producer.close()
  }

  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")
  }
}
