package com.atguigu.hotitem

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object HotItems {
  def main(args: Array[String]): Unit = {
    //1.创建流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置当前时间特性(时间语义)  事件事件
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1) //设置并行度为1

    //是按照数据的时间统计，而不是读取的时间统计!
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
      //分配时间戳 watermark(如果是乱序的话，延迟时间与最大乱序程度有关)
      //乱序使用assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor...)
      .assignAscendingTimestamps(_.timestamp * 1000L) //升序，分配时间戳秒*1000 变毫秒;

    //2. 开窗聚合数据
    val aggregatedStream = dataStream
      .filter(_.behavior == "pv") //过滤出pv行为
      //(keyBy如果使用的是字符串参数，那么它得到的KeyedStream的key的类型是JavaTuple java元组),所以windowfunction中key类型使用Tuple/如果不想这么麻烦，就使用.keyBy(_.itemId)
      .keyBy("itemId") //按照itemId分组，拿到KeyedStream[T, JavaTuple]
      //开窗大小1小时，滑动时间5分钟(得到windowedStream)
      .timeWindow(Time.hours(1), Time.minutes(5)) //开滑动窗口
      //.aggregate(new CountAgg())
      //先做预聚合，然后再输出到WindowFunction中做加上window信息的包装;
      .aggregate(new CountAgg(), new WindowResult()) //窗口聚合操作，结合窗口再处理;

    //3. 排序输出TopN(每个窗口可能重复的id所以要keyBy分组)
    val resultStream = aggregatedStream
      .keyBy("windowEnd") //按照窗口分组 (不同商品在同一窗口的热门排序)
      //接下来涉及到状态编程、排序、定时器设置(等所有数据都到齐了才触发) => 使用自定义processFuntion
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
//参数：(输入,中间聚合计算的状态,输出)
//输出本想要ItemViewCount,但聚合函数中拿不到窗口信息，所以需要将这里结果值传递给window再包装
//AggregateFunction是增量聚合(过程中保持一个状态)
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  //累加器初始值
  override def createAccumulator(): Long = 0L

  //每来一条数据调用一次add，返回为状态变化后的状态;（每来一条数据就加1）=>计数操作
  override def add(in: UserBehavior, acc: Long): Long = acc + 1L

  //返回当前的状态，其实是返回给WindowFunction的
  override def getResult(acc: Long): Long = acc

  //如果涉及到分区的状态累加使用merge，主要在sessionWindow的时候使用到(直接加在一起)
  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

//例子：计算平均数(数据时间戳的平均值)
class AverageAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double] {
  //初始值(0L,0)
  override def createAccumulator(): (Long, Int) = (0L, 0)
  //累加 状态1求和累加，状态计算个数直接加1
  override def add(in: UserBehavior, acc: (Long, Int)): (Long, Int) = (acc._1 + in.timestamp, acc._2 + 1)
  //输出
  override def getResult(acc: (Long, Int)): Double = acc._1 / acc._2
  //如果有多个分区累加，使用到此方法merge
  override def merge(acc: (Long, Int), acc1: (Long, Int)): (Long, Int) = (acc._1 + acc1._2, acc._2 + acc1._2)
}

//自定义窗口函数
//参数：(输入类型,输出类型,key的类型,window类型)
//参数：(CountAgg()的输出是WindowFunction的输入,输出为ItemViewCount样例类,Tuple(keyBy后为字符串时使用Tuple),TimeWindow窗口类型)
class WindowResult() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  //apply方法可以拿到key类型，window类型，和输入为count的个数Long类型(可以是一组但是只用到一个)，输出类型:包装好后使用out输出出去即可
  //window的作用是使用window获取到windowEnd放入到ItemViewCount中
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    //key就是itemId，但是要的是Long类型，传入的是Tuple类型(一元组只有一个参数，需要import 引入Tuple1)，需要把值取出来转换成Long类型
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0 //长整型类型的一元组
    val windowEnd: Long = window.getEnd() //当前window的最小时间戳;The exclusive end timestamp of this window.
    val count: Long = input.iterator.next()
    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}

//如果是DataStream => 使用ProcessFunction 但不能定义定时器
//自定义process function              (因为前面keyBy了)
//类型[key类型，输入类型，输出类型] //屏幕输出
class TopNItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
  //先定义一个列表状态，用来保存当前窗口中所有item的count聚合结果
  private var itemListState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    //参数ListStateDescriptor描述器，item-list和classof => 能唯一确定当前状态;
    //Flink管理状态通过描述其管理，参数item-list表示状态id 和 类型 classof
    //在运行时上下文获取状态的时候就相当于获取到了当前状态的操作句柄;
    itemListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-list", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    //每来一条数据就先缓存起来，用状态缓存
    //第一条数据来的时候，注册一个定时器，延迟1毫秒触发定时器，这个时间到了说明windowEnd前的数据都到了
    itemListState.add(value)

    //注册定时器(因为到此位置，经过keyBy操作，所有的数据都是在一个窗口中的，也就是说他们的windowEnd时间戳相同，同样时间戳的定时器不会重复注册和触发)
    //Flink底层是使用时间戳管理定时器的，所以时间戳就是定时器唯一的id，只要时间戳相同就是同一个定时器且时间到了只触发一次;而且这个定时器只在当前窗口有效;
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  //等到watermark超过windowEnd，达到windowEnd+1，说明所有聚合结果都到齐了，排序输出
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //1. 定义一个本地的List，用于提取所有的聚合的值，方便后面做排序
    val allItemsList: ListBuffer[ItemViewCount] = ListBuffer()
    //itemListState.get()得到的是iteartor类型的;注意get得到的是java实现的类，如果使用scala语言for遍历需要引入import scala.collection.JavaConversions._
    import scala.collection.JavaConversions._
    for (item <- itemListState.get()) {
      allItemsList += item
    }
    //或者直接迭代
    //val allItemsList = itemListState.get().iterator().toList

    //清空list state,释放资源
    itemListState.clear()

    //2. 按照count大小字段进行排序 ,sortBy默认升序（使用函数柯里化操作，反转 Ordering.Long.reverse）
    val sortedItemsList = allItemsList.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    //3. 将排序后列表中的信息，格式化之后输出
    val result: StringBuilder = new StringBuilder() //动态可调整追加
    result.append("==================\n")
    result.append("窗口关闭时间:").append(new Timestamp(timestamp - 1)).append("\n")
    // 用一个for循环遍历sortedItemsList，输出前三名的所有信息
    //for(i<- 0 until sortedItemsList.size ) //这样[0,size)
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
