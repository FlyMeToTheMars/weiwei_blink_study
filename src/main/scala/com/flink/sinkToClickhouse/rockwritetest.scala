package com.flink.sinkToClickhouse

import java.text.SimpleDateFormat
import java.util.{Date, Properties}


import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.rocketmq.flink.{RocketMQConfig, RocketMQSink}
import org.apache.rocketmq.flink.common.selector.DefaultTopicSelector

object rockwritetest {
  def main(args: Array[String]): Unit = {
    //获取环境变量
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    env.setParallelism(1)

   val source = env.addSource(builderTurboMQSource).name("turboMQSource")


    val producerProps = new Properties
    producerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, "192.168.52.72:9876")
    val msgDelayLevel = RocketMQConfig.MSG_DELAY_LEVEL05
    producerProps.setProperty(RocketMQConfig.MSG_DELAY_LEVEL, String.valueOf(msgDelayLevel))
    //   TimeDelayLevel is not supported for batching
    val batchFlag = msgDelayLevel <= 0

    val schema = new SimpleStringSerializationSchema

    val ds = source.map(x=>{
      val id = x._1.split("_")(1).toString
      val province =x._2.split("_")
      val address = province(province.length-1)
     val create_date = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS").format(new Date)
     //Info(id,address,create_date)
     (id,address)
    }).addSink(new RocketMQSink[(String,String)](schema, new DefaultTopicSelector("flink-sink2"), producerProps).withBatchFlushOnCheckpoint(batchFlag))
  // source.print()


    env.execute("test")

  }

  import org.apache.rocketmq.flink.{RocketMQConfig, RocketMQSource}

  private def builderTurboMQSource = {
    val consumerProps = new Properties()
    consumerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, "192.168.52.72:9876")
    consumerProps.setProperty(RocketMQConfig.CONSUMER_GROUP, "c006")
    consumerProps.setProperty(RocketMQConfig.CONSUMER_TOPIC, "flink-source2")
    val schema = new SimpleStringDeserializationSchema
    val turboMQConsumer = new RocketMQSource[(String,String)](schema, consumerProps)
    turboMQConsumer
  }
}
