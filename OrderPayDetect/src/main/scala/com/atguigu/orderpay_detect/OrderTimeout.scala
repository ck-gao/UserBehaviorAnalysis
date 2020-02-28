package com.atguigu.orderpay_detect


import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

//输入数据的样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

//输出订单检测结果样例类(正常、失败)
case class OrderResult(orderId: Long, resultMsg: String)

//实时检测订单15分钟失效 (CEP)
object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //从文件中读取数据
    val resource = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      //      .assignAscendingTimestamps(_.eventTime * 1000L) //升序数据
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(0)) {
        override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
      })
      .keyBy(_.orderId) //根据orderId分组

    //1. 定义一个事件限制的模式
    val orderPayPattern = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))

    //2. 在orderEventStream上应用pattern,生成一个PatternStream
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)

    //3. 定义一个侧输出流，用户把timeout事件输出到侧输出流里去
    val orderTimeoutputTag = new OutputTag[OrderResult]("orderTimeout")

    //4. 调用select方法，得到最终的输出结果
    val resultStream = patternStream.select(orderTimeoutputTag,
      new OrderTimeoutSelect(),
      new OrderPaySelect()
    )

    resultStream.print("payed") //正常支付的事件
    resultStream.getSideOutput(new OutputTag[OrderResult]("orderTimeout")).print("timeout") //支付超时事件
    env.execute("order pay timeout job")
  }
}

//自定义一个PatternTimeoutFunction (使用CEP处理超时的问题，(前面匹配后面不匹配))
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(pattern: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): OrderResult = {
    //选取超时事件
    val timeoutOrderId = pattern.get("create").iterator().next().orderId
    OrderResult(timeoutOrderId, "timeout at " + timeoutTimestamp)
  }
}

//自定义一个 PatternSelectFunction
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId = pattern.get("pay").iterator().next().orderId
    OrderResult(payedOrderId, "payed successfully.")
  }
}
