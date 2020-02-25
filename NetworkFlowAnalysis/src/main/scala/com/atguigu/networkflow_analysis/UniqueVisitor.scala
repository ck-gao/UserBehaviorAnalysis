package com.atguigu.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class UvCount(windowEnd: Long, count: Long)

object UniqueVisitor {
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
      .timeWindowAll(Time.hours(1)) //如果基于DataStream则使用windowall开窗
      .apply(new UvCountByWindow()) //全窗口函数(先拿到所有数据，再统一用set重用)
    //也可以使用增量窗口函数可以实现
    //.aggregate(new PvCountAgg(), new PvResult())

    processedStream.print()
    env.execute("unique visitor job")
  }
}

//自定义全窗口函数
//AllWindowFunction 全窗口可以把数据全部收集起来再做处理  类型没有key [input,output,window]
class UvCountByWindow() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    //用一个set集合类型来保存所有的userId，做到自动去重
    var idSet = Set[Long]()
    //遍历窗口所有数据，全部放入set中
    for (userBehavior <- input) { //input是所有数据
      idSet += userBehavior.userId //每一条数据都存进set，自动去重
    }
    //输出UvCount统计结果
    out.collect(UvCount(window.getEnd, idSet.size))
  }
}
