package com.atguigu.hotitem

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object HotItems {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置当前时间特性(时间语义)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //1. 读取数据
    //val dataStream = env.readTextFile("D:\\workspace\\IdeaProject\\UserBehaviorAnalysis\\HottemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    //kafka源
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val dataStream = env.addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L) //正序，

    //2. 开窗聚合数据
    val aggregatedStream = dataStream
      .filter(_.behavior == "pv") //拿到pv行为
      .keyBy("itemId") //按照itemId分区，拿到KeyedStream
      .timeWindow(Time.hours(1), Time.minutes(5)) //开滑动窗口
      //.aggregate(new CountAgg())
      .aggregate(new CountAgg(), new WindowResult()) //聚合操作后，结合窗口再处理;

    //3. 排序输出TopN(每个窗口可能重复的id所以要keyby)
    val resultStream = aggregatedStream
      .keyBy("windowEnd") //按照窗口分组
      .process(new TopNItems(3)) //自定义ProcessFunction做排序输出

    resultStream.print()
    env.execute()
  }
}

//定义输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

//定义中间聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

//自定义预聚合函数，来一条进行预聚合得到累加状态,把状态值传递给windowfunction做计算即可
//参数：(输入,中间计算状态,输出)
//AggregateFunction是增量聚合(过程中保持一个状态)
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  //初始值
  override def createAccumulator(): Long = 0L

  //每来一条数据调用一次add，返回为状态变化后的状态;（每来一条数据就加1）
  override def add(in: UserBehavior, acc: Long): Long = acc + 1L

  //返回的状态
  override def getResult(acc: Long): Long = acc

  //分区累加，主要在sessionWindow的时候使用到(直接加在一起)
  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

//自定义窗口函数
//参数：(CountAgg()的输出是WindowFunction的输入,输出为ItemViewCount样例类,Tuple(keyBy后为字符串时使用Tuple),TimeWindow)
class WindowResult() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0 //长整型类型的一元组
    val windowEnd: Long = window.getEnd() //当前window的最小时间戳
    val count: Long = input.iterator.next()
    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}

//自定义process function               (因为前面keyBy了)
class TopNItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
  //先定义一个列表状态，用来保存当前窗口中所有item的count聚合结果
  private var itemListState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    //item-list和classof  能唯一确定当前状态
    itemListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-list", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    //每来一条数据就先缓存起来，用状态缓存
    //第一条数据来的时候，注册一个定时器，延迟1毫秒，这个时间到了说明windowEnd前的数据都到了
    itemListState.add(value)
    //注册定时器(可以直接每个数据都注册一个定时器，因为定时器是以时间戳作为id的，同样时间戳的定时器不会重复注册和触发)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  //等到watermark超过windowEnd，达到windowEnd+1，说明所有聚合结果都到齐了，排序输出
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //1. 定义一个本地的List，用于提取所有的聚合的值，方便后面做排序
    val allItemsList: ListBuffer[ItemViewCount] = ListBuffer()
    //Java到scala;
    import scala.collection.JavaConversions._
    for (item <- itemListState.get()) {
      allItemsList += item
    }
    //清空list state,释放资源
    itemListState.clear()

    //2. 按照count大小进行排序 ,sortBy默认升序（使用柯里化操作，反转）
    val sortedItemsList = allItemsList.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    //3. 将排序后列表中的信息，格式化之后输出
    val result: StringBuilder = new StringBuilder()
    result.append("==================\n")
    result.append("窗口关闭时间:").append(new Timestamp(timestamp - 1)).append("\n")
    // 用一个for循环循环遍历sortedItemsList，输出前三名的所有信息
    for (i <- sortedItemsList.indices) { //indices遍历所有下标
      val currenItem = sortedItemsList(i)
      result.append("No").append(i + 1).append(":")
        .append("商品ID=").append(currenItem.itemId)
        .append("浏览量=").append(currenItem.count)
        .append("\n")
    }
    //控制显示频率
    Thread.sleep(1000L)
    //最终输出
    out.collect(result.toString())
  }
}
