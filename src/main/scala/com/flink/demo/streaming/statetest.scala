package com.flink.demo.streaming

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

/**
  * @author Wang.long
  * @date 2019/11/10 23:03
  * @Description * @Mail wang.long@ustcinfo.com
  */
class CountWindowAverage extends RichFlatMapFunction[(Long, Long), (Long, Long)] {

  private var sum: ValueState[(Long, Long)] = _

  override def flatMap(input: (Long, Long), out: Collector[(Long, Long)]): Unit = {

    // access the state value
    val tmpCurrentSum = sum.value

    // If it hasn't been used before, it will be null
    //初始化
    val currentSum = if (tmpCurrentSum != null) {
      tmpCurrentSum
    } else {
      (0L, 0L)
    }

    // update the count
    // 新的结果
    val result = (currentSum._1 + 1, input._2-currentSum._2)
    //val newSum = (currentSum._1 + 1, currentSum._2 + input._2)

    // update the state
    //sum.update(result)

    // if the count reaches 2, emit the average and clear the state
    if (result._1 >= 2) {
      out.collect((input._1,result._2))
      sum.clear()
    }
    if(result._1==1){
      sum.update(result._1,input._2)
    }
  }

  override def open(parameters: Configuration): Unit = {
    sum = getRuntimeContext.getState(
      new ValueStateDescriptor[(Long, Long)]("average", createTypeInformation[(Long, Long)])
    )
  }
}


object ExampleCountWindowAverage extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.fromCollection(List(
    (2l, 3L),
    (1L, 5L),
    (2l, 7L),
    (1L, 6L),
    (3L, 2L),
    (3L, 8L)
  )).keyBy(_._1)
    .flatMap(new CountWindowAverage())
    .print()
  // the printed output will be (1,4) and (1,5)

  env.execute("ExampleManagedState")
}