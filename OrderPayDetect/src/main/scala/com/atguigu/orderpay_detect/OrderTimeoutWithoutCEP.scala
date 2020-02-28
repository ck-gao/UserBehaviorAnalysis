package com.atguigu.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

//实时检测订单15分钟失效 (ProcessFunction)
object OrderTimeoutProcessFunction {
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
      //.assignAscendingTimestamps(_.eventTime * 1000L) //升序数据
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(0)) {
        override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
      })
      .keyBy(_.orderId) //根据orderId分组

    //状态编程，实现正常支付订单和超时订单的检测和分流
    val orderResultStream = orderEventStream
      .process(new OrderPayMatch())

    val orderTimeoutOutoutTag = new OutputTag[OrderResult]("orderTimeout")
    orderResultStream.print("payed")
    orderResultStream.getSideOutput(orderTimeoutOutoutTag).print("timeout")

    env.execute("order timeout detect without cep job")
  }
}

// 自定义处理订单超时事件的Process Function
class OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
  //定义状态，用于表示当前订单的create事件和pay事件是否来过
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-payed", classOf[Boolean]))
  lazy val isCreatedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-created", classOf[Boolean]))

  //定义状态，用来保存注册定时器的时间戳；如果中间来了pay事件，删除定时器
  lazy val timerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

  // 把相同的侧输出流标签定义好
  val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
    //先获取当前状态
    val isPayed = isPayedState.value()
    val isCreated = isCreatedState.value()
    val timerTs = timerState.value()

    //分情况讨论：来的数据是create还是pay
    //情况1：来的是create; 要判断create之前是否pay过？因为可能有乱序数据
    if (value.eventType == "create") {
      //1.1 继续判断，如果pay过，匹配成功
      if (isPayed) {
        out.collect(OrderResult(value.orderId, "payed successfully")) //主流
        //清空状态，删除定时器
        isPayedState.clear()
        timerState.clear()
        ctx.timerService().deleteEventTimeTimer(timerTs) //时间戳为:取出来的时间戳
      }
      //1.2 如果没有pay过，注册定时器，等待15分钟
      else {
        val ts = value.eventTime * 1000L + 15 * 60 * 1000L //注册定时器的时间戳
        ctx.timerService().registerEventTimeTimer(ts)
        //更新状态
        isCreatedState.update(true) //来了创建create状态，更新状态
        timerState.update(ts) // 更新时间戳
      }
    }
    //情况2: 来的是pay，还要判断是否create过
    else if (value.eventType == "pay") {
      //2.1 判断如果create了，正常应该匹配成功，另外还需考虑已经超时，但乱序来的pay事件
      if (isCreated) {
        //还需判断pay的时间戳是否超过15分钟
        if (value.eventTime * 1000L < timerTs) {
          //2.1.1 如果没有超时，正常匹配
          out.collect(OrderResult(value.orderId, "payed sucessfully"))
        } else {
          //2.1.2 已经超时，输出到侧输出流，超时报警
          ctx.output(orderTimeoutOutputTag, OrderResult(value.orderId, "payed but timeout"))
        }
        // 已经输出，清理状态
        isCreatedState.clear()
        timerState.clear()
        ctx.timerService().deleteEventTimeTimer(timerTs)
      }
      // 2.2 没有create过，说明乱序pay先到了，等待
      else {
        // watermark涨到定时器时间戳,定时器才触发
        // 注册定时器等待create,等待pay的时间戳就可以了
        ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L)
        // 更新状态
        isCreatedState.update(true)
        timerState.update(value.eventTime * 1000L)
      }
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    // 定时器触发，有一个没到
    if (isPayedState.value()) {
      // create没来，输出异常报警
      ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "payed but not found create"))
    } else if (isCreatedState.value()) {
      // pay没来，真正的超时报警
      ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "timeout"))
    }
    // 清理状态
    isPayedState.clear()
    isCreatedState.clear()
    timerState.clear()
  }
}

