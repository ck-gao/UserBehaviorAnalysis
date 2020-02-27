package com.atguigu.market_analysis

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


//输入log数据样例类
case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

//输出按省份分类统计结果样例类
case class AdCountByProvince(windowEnd: String, province: String, count: Long)

//黑名单报警样例类
case class BlackListWarning(userId: Long, adId: Long, msg: String)

object AdAnalysis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource = getClass.getResource("/AdClickLog.csv")
    val adLogStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        AdClickLog(dataArray(0).toLong, dataArray(1).toLong, dataArray(2), dataArray(3), dataArray(4).toLong)

      })
      .assignAscendingTimestamps(_.timestamp * 1000) //Flink使用毫秒数据处理，所以*1000

    //黑名单过滤，侧输出流报警
    val filterBlackListStream = adLogStream
      .keyBy(data => (data.userId, data.adId)) //以(userid和adid)二元组分组
      //使用.filter(new MyRichFilter()) // 使用富函数编程也可以，可以过滤，但是还要侧输出流输出;所以使用process function
      .process(new FilterBlackListUser(100))

    //开窗聚合 (没有问题的数据发送过来)
    val adCountStream = filterBlackListStream
      .keyBy(_.province) //省份分组
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountResult()) //增量聚合
    //自定义预聚合函数，自定义窗口处理函数

    adCountStream.print("count") //正常数据
    filterBlackListStream.getSideOutput(new OutputTag[BlackListWarning]("blacklist")).print("black-list")
    env.execute("ad analysis job")
  }
}

class AdCountAgg() extends AggregateFunction[AdClickLog, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: AdClickLog, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}


class AdCountResult() extends WindowFunction[Long, AdCountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdCountByProvince]): Unit = {
    //val end: Timestamp = new Timestamp(window.getEnd)
    out.collect(AdCountByProvince(formatTs(window.getEnd), key, input.iterator.next()))
  }

  private def formatTs(ts: Long) = {
    import org.apache.flink.util.Collector
    val df = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")
    df.format(new Date(ts))
  }
}

//自定义keyedProcessFunction
class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {
  //定义状态:用来保存当前用户对当前广告的点击量
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
  //定义状态:标记当前用户是否已经进入黑名单
  lazy val isSentState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-send", classOf[Boolean]))
  //定义状态:保存0点触发的定时器时间戳
  lazy val resetTimerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-timer", classOf[Long]))

  override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
    //从状态中取出当前的Count值
    val curCount = countState.value()

    //统计一段时间(每天)的点击量，每天0点清空一次状态
    //如果是第一次点击，那么注册定时器，第二天0点触发清除状态
    if (curCount == 0) {
      //获得明天0点时间戳
      val ts = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24) //ms/1000/60/60/24 = 天数
      ctx.timerService().registerEventTimeTimer(ts) //注册定时器
      resetTimerState.update(ts)
    }
    //判断如果当前count超过了上限，那么加入黑名单，侧输出流报警
    if (curCount >= maxCount) {
      //判断是否发到过黑名单，如果已经发过就不用发了
      if (!isSentState.value()) { //为false 继续后面操作
        ctx.output(new OutputTag[BlackListWarning]("blacklist"),BlackListWarning(value.userId, value.adId, "Click over " + maxCount))
        isSentState.update(true) //发送以后，状态置为true
      }
      //return //没超过则直接返回，或用下面else
    } else { //如果没达到上限则正常输出到主流
      countState.update(curCount + 1)
      out.collect(value)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
    //如果是0点的时间，那么触发清空操作
    //可以直接判断 resetTimerState触发定时器的状态
    if (timestamp == resetTimerState.value()) {
      isSentState.clear()
      countState.clear()
      resetTimerState.clear()
    }
  }
}
