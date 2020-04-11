package com.flink.demo.mysql

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import com.flink.utils.MysqlSource.getMysqlSource
/**
  * author Renault
  * date 2019/12/17 16:39
  * Description 
  */
object mysqlStreaming {
  def main(args: Array[String]): Unit = {
    //注册流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置 时间类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val planner = "blink"
    //获取使用的flink分支
    val tEnv = if (planner == "blink") {  // use blink planner in streaming mode
      val settings = EnvironmentSettings.newInstance()
        .useBlinkPlanner()
        .inStreamingMode()
        .build()
      StreamTableEnvironment.create(env, settings)
    } else if (planner == "flink") {  // use flink planner in streaming mode
      StreamTableEnvironment.create(env)
    } else {
      System.err.println("The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', " +
        "where planner (it is either flink or blink, and the default is flink) indicates whether the " +
        "example uses flink planner or blink planner.")
      return
    }

//    //获取mysql 数据
    val sql = "select * from Cource"
    val source = getMysqlSource(env,10,sql)

    source.print()

    env.execute("test")
  }

}
