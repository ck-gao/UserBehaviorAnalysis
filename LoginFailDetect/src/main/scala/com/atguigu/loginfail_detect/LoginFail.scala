package com.atguigu.loginfail_detect

import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

//输入报警信息样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, msg: String)
//恶意登录监控
object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource = getClass.getResource("/LoginLog.csv")
    val adLogStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(6)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L //获取时间戳
      })

    //检测连续登录失败，输出报警信息
    val warningStream = adLogStream
      .keyBy(_.userId) //按照userId分组;  _.userId避免Tuple类型
      .process(new MatchFunction(2))

    warningStream.print()
    env.execute("login fail job")
  }
}

//自定义实现processFunction，将之前的登录失败事件存入状态，注册定时器，判断状态中有几次登录失败
class MatchFunction(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {
  //定义状态,ListState,用于保存已经到达的连续登录失败事件
  lazy val loginFailListState = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("saved-loginfails", classOf[LoginEvent]))
  //可以再使用一个状态，保存定时器触发的状态(略)

  //来一条数据执行一次processElement
  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    //判断事件类型，只添加登录失败事件；如果是成功事件，直接清空状态;
    if (value.eventType == "fail") {
      /*
      loginFailListState.add(value)
      //注册定时器,根据当前时间设定2秒后触发  //这里面没有做判断是否是第一次？
      ctx.timerService().registerEventTimeTimer(value.eventTime * 1000 + 2000L)
     */
      //优化，判断之间是否已有登录失败事件，如果有，判断事件戳差值；如果没有，把当前第一个失败事件存入状态
      val iter = loginFailListState.get().iterator()
      if (iter.hasNext) {
        //如果已经有一次登录失败了那么取出跟当前登录失败取差值，差值小于打印
        val firstFailEvent = iter.next()
        if ((firstFailEvent.eventTime - value.eventTime).abs < 2) {
          //如果在两秒以内，输出报警信息
          out.collect(Warning(value.userId, firstFailEvent.eventTime, value.eventTime, "login fail 2 times"))
        }
        //如果当前时间戳大于上次登录失败时间戳，更新最近的登录失败事件;
        if (value.eventTime > firstFailEvent.eventTime) {
          //value更近一点，可以先清空再传入;
          loginFailListState.clear()
          loginFailListState.add(value)
        }
      } else {
        //如果之前没有失败事件，那么当前事件是第一个，直接添加到状态
        loginFailListState.add(value)
      }
    } else {
      //如果中间遇到了成功事件，清空状态重新开始
      loginFailListState.clear()
    }
  }

  /*
    //改进后不使用定时器
    //2秒后触发定时器
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#OnTimerContext, out: Collector[LoginEvent]): Unit = {
      import scala.collection.JavaConversions._
      //先从listState中取出所有的登录失败事件
      val allLoginFails = loginFailListState.get().iterator().toList
      //判断总共的失败事件是否超过maxFailTime
      if (allLoginFails.length >= maxFailTimes) {
        //输出报警信息
        out.collect(Warning(allLoginFails.head.userId, allLoginFails.head.eventTime, allLoginFails.last.eventTime, "login fail in 2s for " + allLoginFails.length + " times"))
      }
      loginFailListState.clear() //清空状态(不管报没报警，2秒后都清除状态)
      //还需要删除定时器
    }
   */
}
