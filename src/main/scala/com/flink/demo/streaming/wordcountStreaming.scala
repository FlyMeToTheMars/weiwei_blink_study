package com.flink.demo.streaming

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * author Renault
  * date 2019/12/17 10:46
  * Description 
  */
object wordcountStreaming {
  def main(args: Array[String]): Unit = {
    //获取参数
    val params = ParameterTool.fromArgs(args)
    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)



  }

}
