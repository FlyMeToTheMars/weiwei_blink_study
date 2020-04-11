package com.flink.sinkToClickhouse

import java.util
import java.util.Properties

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.rocketmq.flink.{RocketMQConfig, RocketMQSource}
import org.apache.rocketmq.flink.common.serialization.SimpleKeyValueDeserializationSchema
import org.apache.flink.api.scala._

/**
  * author Renault
  * date 2019/12/25 14:44
  * Description 
  */
object RockmqtoClickhouse {
  def main(args: Array[String]): Unit = {

    //获取环境变量
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    env.setParallelism(1)
    val consumerProps = new Properties()
    consumerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, "182.10.1.144:9876")
    consumerProps.setProperty(RocketMQConfig.CONSUMER_GROUP, "c002")
    consumerProps.setProperty(RocketMQConfig.CONSUMER_TOPIC, "flink-source2")

   val source =
         env.addSource(new RocketMQSource(new SimpleKeyValueDeserializationSchema("id", "address"), consumerProps))
          .name("rocketmq-source")
          .setParallelism(2)

//    source.map(x=>{
//      val id = x.get("id")
//      val province = x.get("address")
//      (id,province)
//    }).print()

//    source.process(new ProcessFunction[util.Map[_,_],(Int,String)]{
//      override def processElement(i: util.Map[_, _], context: ProcessFunction[util.Map[_, _], (Int,String)]#Context, collector: Collector[(Int,String)]) = {
//        val id  = i.get("id").toString.toInt
//        val arr = i.get("address").toString.split("\\s+")
//        val value =arr(arr.length-1)
//
//        collector.collect((id,value))
//      }
//    }).print()
//      .process(new ProcessFunction[Map[_, _], Map[_, _]]() {
//      @throws[Exception] override def processElement(in: Map[_, _], ctx: ProcessFunction[Map[_, _], Map[_, _]]#Context, out: Collector[Map[_, _]]): Unit = {
//        val result = new util.HashMap[_, _]
//        result.put("id", in.get("id"))
//        val arr = in.get("address").toString.split("\\s+")
//        result.put("province", arr(arr.length - 1))
//        out.collect(result)
//      }
//    })
   // source.print()

    env.execute("test")


  }



}


