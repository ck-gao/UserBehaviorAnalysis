package com.atguigu.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


//输入数据样例类，OrderEvent用之前的
case class ReceiptEvent(txId: String, payChannel: String, eventTime: Long)

//使用双流合并实现 支付对账
object OrderTxMatch {

  //为了公用OutputTag,直接定义出来
  val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")
  val unamtchedReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据，来自Order和Receipt两条流
    val orderPayResource = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(orderPayResource.getPath)
      //val orderEventStream = env.socketTextStream("hadoop102",777)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(0)) {
        override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
      })
      .filter(_.txId != "") // 过滤出txId不为空的订单支付事件
      .keyBy(_.txId) //用交易号分组进行两条流的匹配

    val receiptResource = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream = env.readTextFile(receiptResource.getPath)
      //val receiptEventStream = env.socketTextStream("hadoop102", 8888)
      .map(data => {
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(0)) {
        override def extractTimestamp(element: ReceiptEvent): Long = element.eventTime * 1000L
      })
      .keyBy(_.txId) // 用交易号分组进行两条流的匹配

    //合流并处理
    val processedStream = orderEventStream.connect(receiptEventStream)
      .process(new OrderTxPayMatch())

    processedStream.print("matched")
    processedStream.getSideOutput(unmatchedPays).print("unmatched pays")
    processedStream.getSideOutput(unamtchedReceipts).print("unmatched receipts")
    env.execute("order tx match")
  }

  class OrderTxPayMatch() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
    // 用两个value state，来保存当前交易的支付事件和到账事件
    lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("order-pay", classOf[OrderEvent]))
    lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("tx-receipt", classOf[ReceiptEvent]))

    override def processElement1(value: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      //订单事件来了，需要考虑是否有对应的到账事件
      val receipt = receiptState.value()
      if (receipt != null) {
        //如果已经有对应的receipt，匹配输出到主流
        out.collect((value, receipt))
        receiptState.clear()
      } else {
        //如果receipt还没来，存储pay状态，注册定时器等待
        payState.update(value)
        ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 5000L) // 等待5秒，receipt还不来就输出报警
      }
    }

    override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      //到账事件来了，需要考虑是否有对应的支付事件
      val pay = payState.value()
      if (pay != null) {
        //如果已经有对应的pay，匹配输出到主流
        out.collect((pay, receipt))
        payState.clear()
      } else {
        //如果pay还没来，存储receipt状态，注册定时器等待
        receiptState.update(receipt)
        ctx.timerService().registerEventTimeTimer(receipt.eventTime * 1000L + 3000L) //等待3秒，pay还不来就输出报警
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      //定时器触发，一定有一个还没到，输出不匹配报警信息
      if (payState.value() != null) {
        //pay有值，说明receipt没到
        ctx.output(unmatchedPays, payState.value())
      } else if (receiptState.value() != null) {
        ctx.output(unamtchedReceipts, receiptState.value())
      }
      //只要有报警信息就清除状态
      payState.clear()
      receiptState.clear()
    }
  }

}
