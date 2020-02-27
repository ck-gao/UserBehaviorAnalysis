package com.atguigu.loginfail_detect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

//恶意登录 - CEP编程
object LoginFailWithCEP {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 1. 读取数据，准备 dataStream
    val resource = getClass.getResource("/LoginLog.csv")
    val loginEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(6)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L //获取时间戳
      })
      .keyBy(_.userId)

    //2. 定义一个Pattern 用来检测dataStream里的连续登录失败事件
    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern
      .begin[LoginEvent]("firstFail").where(_.eventType == "fail") //第一次登录失败
      .next("secondFail").where(_.eventType == "fail") //第二次登录失败
      .within(Time.seconds(5)) //5秒内有效

    //3. 将Pattern应用于当前的数据流，得到一个PatternStream
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream, loginFailPattern)

    //4. 定义一个SelectFunction ,从检测到匹配的复杂事件流中提取事件，输出报警信息
    val warningStream = patternStream.select(new LoginFailMatch())

    warningStream.print()
    env.execute("login fail detect with cep job")
  }
}

//自定义SelectFunction，输出连续登录失败的报警信息
//每个匹配成的事件都会调用select，都会放到Map中;
class LoginFailMatch() extends PatternSelectFunction[LoginEvent, Warning] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    //从组合事件map中提取匹配好的单个事件
    val firstFailEvent = map.get("firstFail").iterator().next()
    val secondFailEvent = map.get("secondFail").iterator().next()
    Warning(firstFailEvent.userId, firstFailEvent.eventTime, secondFailEvent.eventTime, "login fail")
  }
}

