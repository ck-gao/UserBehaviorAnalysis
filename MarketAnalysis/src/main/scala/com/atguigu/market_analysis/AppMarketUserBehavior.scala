package com.atguigu.market_analysis

import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random


//定义输入数据样例类
case class MarketUserBebavior(userId: String, behavior: String, channel: String, timestamp: Long)

//定义一个统计后输出的样例类
case class MarkViewCountByChannel(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

//自定义数据源   (并行产生数据)
class SimulatedDataSource() extends RichParallelSourceFunction[MarketUserBebavior] {
  //是否运行标识位
  var running = true
  //定义出推广取到和用户行为的集合
  val channelSet: Seq[String] = Seq("AppStroe", "HuaWeiStor", "XiaoMiStor", "WeiBo", "wechat")
  val behaviorSet: Seq[String] = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
  //定义一个随机数生成器
  val rand: Random = new Random()

  override def run(ctx: SourceFunction.SourceContext[MarketUserBebavior]): Unit = {
    val maxElements = Long.MaxValue
    var count = 0L
    //无限循环，生成随机数据
    while (running && count < maxElements) {
      val id = UUID.randomUUID().toString
      val behavior = behaviorSet(rand.nextInt(behaviorSet.size))
      val channel = channelSet(rand.nextInt(channelSet.size))
      val ts = System.currentTimeMillis()
      ctx.collect(MarketUserBebavior(id, behavior, channel, ts))
      count += 1
      Thread.sleep(10)
    }
  }

  override def cancel(): Unit = false
}

object AppMarketUserBehavior {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //生成数据
    val dataStream: DataStream[MarketUserBebavior] = env.addSource(new SimulatedDataSource())
      .assignAscendingTimestamps(_.timestamp) //升序

    //开窗聚合
    val processedStream = dataStream
      .filter(_.behavior != "UNINSTALL")
      //.keyBy(1, 2) //方式1：behavior 和 channel => groupBy
      //.keyBy("behavior","channel") // 方式2
      .keyBy(data => (data.channel, data.behavior)) //方式3
      .timeWindow(Time.hours(1), Time.seconds(5))
      .process(new MarketCountByChannel())
      //什么时候用aggregate?增量聚合，速度快;
      //什么时候用process? 获取上下文时使用，状态编程，时间信息，使用processfunction

    processedStream.print()
    env.execute()
  }
}

//ProcessWindowFunction能拿到上下文   //WindowFunction只能获取到window窗口信息
class MarketCountByChannel() extends ProcessWindowFunction[MarketUserBebavior, MarkViewCountByChannel, (String, String), TimeWindow] {
  override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBebavior], out: Collector[MarkViewCountByChannel]): Unit = {
    //从上下文中获取window信息，包装成样例类
    val start = new Timestamp(context.window.getStart).toString
    val end = new Timestamp(context.window.getEnd).toString
    out.collect(MarkViewCountByChannel(start, end, key._1, key._2, elements.size))
  }
}
