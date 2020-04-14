package com.flink.demo.batchDemo

import org.apache.flink.api.scala._

/**
  * author Renault
  * date 2019/12/9 9:23
  * Description flinkWordCount
  */
object wordCount {
  def main(args: Array[String]): Unit = {
    /**
      * 批处理场景
      * 构建批处理环境
      */
    val env = ExecutionEnvironment.getExecutionEnvironment
    //测试数据
    val text = env.fromElements(
      "To be, or not to be,--that is the question:--",
      "Whether 'tis nobler in the mind to suffer",
      "The slings and arrows of outrageous fortune",
      "Or to take arms against a sea of troubles,")

    //计算
    val result = text.flatMap(_.toLowerCase.split("\\W"))
      .map((_,1))
      .groupBy(0)
      .sum(1)

    result.print()

    env.execute("action")
  }
}
