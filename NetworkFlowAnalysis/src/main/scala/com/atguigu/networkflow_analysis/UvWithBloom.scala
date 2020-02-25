package com.atguigu.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

object UvWithBloom {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass().getResource("/UserBehavior.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
      })
      //升序数据
      .assignAscendingTimestamps(_.timestamp * 1000L)


    val processedStream = dataStream
      .filter(_.behavior == "pv")
      .map(data => ("uv", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1)) //如果基于DataStream则使用windowall开窗
      .trigger(new MyTrigger()) //自己定义窗口触发规则,来一条触发一次process
      .process(new UvCountWithBloom())

    processedStream.print()
    env.execute("unique visitor Blomm job")
  }
}

//自定义触发器
class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
  //来一个元素，都触发一次窗口计算操作
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.FIRE_AND_PURGE //触发并清空窗口中状态
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //TriggerResult.FIRE //只触发不关闭
    //TriggerResult.FIRE_AND_PURGE //触发窗口计算，并关闭窗口
    TriggerResult.CONTINUE //来了什么都不处理
  }

  //当时间改变了，要不要做处理？
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}

//自定义process window function
class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {
  //创建redis连接和布隆过滤器
  lazy val jedis = new Jedis("hadoop102", 6379)
  // 2^29 = 512M  (5亿多位对应数据容量是512M),对应redis中存储大小位2^29bit=2&26 Byte = 64MB空间
  //redis 64M大小  对应 512M数据大小 =>  5亿个数据量
  lazy val bloom = new Bloom(1 << 29)

  //特殊的全窗口函数
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    //在redis里存储的位图，每个窗口都以windowEnd作为位图的key
    val storeKey = context.window.getEnd.toString
    //定义当前窗口的uv count值,count值也存在redis中用hashmap存储(表名count);
    var count = 0L
    if (jedis.hget("count", storeKey) != null) {
      count = jedis.hget("count", storeKey).toLong
    }
    //对userId取hash值得到偏移量，查看bitmap中是否存在
    val userId = elements.last._2.toString
    val offset = bloom.hash(userId, 61)

    //用redis命令查询bitmap
    val isExist = jedis.getbit(storeKey, offset)
    if (!isExist) {
      //如果不存在，那么将对应位置置1，然后count + 1
      jedis.setbit(storeKey, offset, true)
      jedis.hset("count", storeKey, (count + 1).toString)
      //可以输出查看UvCount
      out.collect(UvCount(storeKey.toLong, count + 1))
    } else {
      out.collect(UvCount(storeKey.toLong, count))
    }
  }
}

//自定义布隆过滤器
class Bloom(size: Long) extends Serializable {
  //容量是2的n次方
  private val cap = size

  def hash(value: String, seed: Int): Long = {
    var result = 0
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)
    }
    //返回hash,需要再cap范围内
    (cap - 1) & result
  }
}







