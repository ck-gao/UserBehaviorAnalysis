package com.atguigu.networkflow_analysis


import java.net.URL
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//定义一个输入数据的样例类
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

//定义中间的聚合结果样例类
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object HotPagAnalysis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据
    val url: URL = getClass.getResource("/apache.log")
    val filrPath: String = url.getPath
    //    val dataStream = env.readTextFile(filrPath)
    val dataStream = env.socketTextStream("hadoop102", 7777)
      .map(data => {
        val dataArray = data.split(" ")
        //对事件事件进行转换，得到Long的时间戳
        val simpleDataFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp: Long = simpleDataFormat.parse(dataArray(3)).getTime()
        ApacheLogEvent(dataArray(0), dataArray(1), timestamp, dataArray(5), dataArray(6))
      })
      //这里数据是乱序数据,watermark,做了：提取时间字段和设置延迟时间；watermark为1秒钟延迟
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
      })

    //开窗聚合
    val aggregateStream = dataStream
      .filter(data => { //过滤掉资源文件.css .js
        val pattern = "^((?!\\.(css|js|png)$).)*$".r
        (pattern findFirstIn data.url).nonEmpty
      })
      .filter(_.method == "GET") //过滤出GET请求
      .keyBy(_.url)
      //.keyBy("url") //按照url分组
      //第一个10为窗口大小10分钟，第二个10为滑动步长
      .timeWindow(Time.minutes(10), Time.seconds(10))
      //开窗口允许窗口延迟时间，也就是watermark到了以后可以输出一下结果但是不关窗，后面来数据继续更新数据知道窗口延迟时间到;
      .allowedLateness(Time.minutes(1))
      //侧输出流，等待窗口延迟时间到了以后，再有数据来则放到侧输出流(测输出流id标签为late)
      .sideOutputLateData(new OutputTag[ApacheLogEvent]("late"))
      //watermark+1时间到了之后会触发聚合操作，但不关闭，因为上面设置了allowedLateness；
      //窗口延迟期间，中间的聚合状态和定时器等都会保持，知道窗口关闭时间到了
      .aggregate(new UrlCountAgg(), new UrlCountResult()) //聚合操作，UrlCountAgg类中统计好的值传递给UrlCountResult进行包装

    //排序输出
    val resultStream = aggregateStream
      //.keyBy("windowEnd") //按照窗口分组
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))

    resultStream.print("result") //结果数据
    dataStream.print("input") //输入的数据转换成样例类输出一下
    aggregateStream.print("agg") //聚合结果输出一下
    aggregateStream.getSideOutput(new OutputTag[ApacheLogEvent]("late")).print() //迟到输出测输出流输出
    env.execute("hot page job")
  }
}

//自定义预聚合函数
class UrlCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  //来一条数据就加1
  override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

//自定义窗口函数，包装成样例类 [预聚合的输出作为输入,窗口函数输出,key类型,窗口类型]
class UrlCountResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    //val url: String = key.asInstanceOf[Tuple1[String]]._1 //如果是Tuple类型
    val count = input.iterator.next
    out.collect(UrlViewCount(key, window.getEnd, count))
  }
}

//自定义processfunction，因为是keyBy之后的，所以要继承KeyedProcessFunction
class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
  //先定义一个List状态，用来保存所有的聚合结果;   通过运行时上下文获取状态句柄(参数是描述器用来唯一确定这个状态)
  //lazy val urlListState: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url-list", classOf[UrlViewCount]))

  //优化不使用List，url去重，用MapState来保存聚合结果，用于迟到数据更新聚合结果时去重
  lazy val urlMapState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String,Long]("url-Map",classOf[String],classOf[Long]))

  //每来一个元素都会调用processElement方法进行处理;
  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    //每条数据直接放入List state中
    //urlListState.add(value)
    urlMapState.put(value.url,value.count) //直接更新，因为有乱序数据，所有后面来的数据值肯定大于以前的count值

    //定义一个windowEnd + 1的定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  //定时器触发时：watermark涨过了windowEnd，所以所有的聚合结果都到了
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //从状态中获取所有聚合结果
//    val allUrlsCount: ListBuffer[UrlViewCount] = new ListBuffer[UrlViewCount]
//    val iter = urlListState.get().iterator() //获取迭代器
//
//    while (iter.hasNext) { //iter迭代器还有后面数据
//      allUrlsCount += iter.next()
//    }
//    urlListState.clear() //清空状态
    //优化，不适用List
    val allUrlsCount: ListBuffer[(String,Long)] = new ListBuffer[(String,Long)]()
    val iter = urlMapState.entries().iterator()  //entries()拿到(key,value)元组
    while(iter.hasNext){
      val entry = iter.next()
      allUrlsCount += ((entry.getKey,entry.getValue)) //不能使用一个括号，一个括号的话会将二元组当成一个参数传进去
    }
    //这里不能clear,因为可能还会有其他窗口的状态值，所以不能clear;什么时候清空呢？等到窗口延迟1分钟到了之后再清空,可以再定义一个定时器来做判断

    //sortWith()的参数是一个bool类型的函数(前面count>后面count)
    //val sortedUrlsCount = allUrlsCount.sortWith(_.count > _.count).take(topSize)
    //优化，因为时二元组所以使用 _._2
    val sortedUrlsCount = allUrlsCount.sortWith(_._2 > _._2).take(topSize)

    //3. 将排序后列表中的信息，格式化之后输出
    val result: StringBuilder = new StringBuilder //动态可调整追加

    result.append("==================\n")
    result.append("窗口关闭时间:").append(new Timestamp(timestamp - 1)).append("\n")
    // 用一个for循环遍历sortedUrlsCount，输出前三名的所有信息
    //for(i<- 0 until sortedUrlsCount.size ) //这样[0,size)
    for (i <- sortedUrlsCount.indices) { //indices遍历所有下标
      val currenUrl = sortedUrlsCount(i)
      result.append("No").append(i + 1).append(":")
        .append("URL=").append(currenUrl._1) //url
        .append("访问量=").append(currenUrl._2) //count
        .append("\n")
    }
    //控制显示频率
    Thread.sleep(1000L)
    //最终输出
    out.collect(result.toString())
  }
}

