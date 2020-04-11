package com.flink.demo.batchDemo

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.api.scala._

/**
  * author Renault
  * date 2019/12/10 15:28
  * Description 
  */
object wordCountWithBlink {
  def main(args: Array[String]): Unit = {
    /**
      * 设置环境
      */
    val env = ExecutionEnvironment.getExecutionEnvironment

    /**
      * 准备数据
      */
    val words = env.readTextFile("data/word.txt")
      .flatMap(_.split(",")).map((_,1))

    val bbSettings = EnvironmentSettings.newInstance.useBlinkPlanner.inBatchMode.build()
    // 创建一个使用 Blink Planner 的 TableEnvironment, 并工作在流模式

//    val bbTableEnv = TableEnvironment.create(bbSettings)
    



  }

}
