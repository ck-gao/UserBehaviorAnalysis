package com.atguigu.networkflow_analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//定义样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

//pv统计输出的样例类 (windowEnd目的是开窗统计多长时间的计数值count)
case class PvCount(windowEnd: Long, count: Long)

object PageView {
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

    //开窗聚合
    val processedStream = dataStream
      .filter(_.behavior == "pv") //选取pv行为
      .map(data => ("pv", 1)) //map成所有数据拥有共同的key，然后统一做聚合
      .keyBy(_._1) //分组
      .timeWindow(Time.hours(1)) //滚动窗口，统计每个小时的pv量
      .aggregate(new PvCountAgg(), new PvResult()) //增量聚合函数，将聚合值传递给PvResult包装window信息输出

    processedStream.print()
    env.execute("page view job")
  }
}

//自定义预聚合函数                            [input,中间聚合状态,输出]
class PvCountAgg() extends AggregateFunction[(String, Int), Long, Long] {
  override def createAccumulator(): Long = 0L
  override def add(in: (String, Int), acc: Long): Long = acc + 1
  override def getResult(acc: Long): Long = acc
  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

//自定义窗口函数                         [input,output,key,window]
class PvResult() extends WindowFunction[Long, PvCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
    //输出
    out.collect(PvCount(window.getEnd, input.iterator.next()))
  }
}




