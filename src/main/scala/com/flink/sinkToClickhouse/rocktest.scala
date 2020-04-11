package com.flink.sinkToClickhouse

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Types}
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.types.Row
import com.flink.utils.ClickHouseSink._

object rocktest {

  def main(args: Array[String]): Unit = {
    //获取环境变量
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    env.setParallelism(1)

   val source = env.addSource(builderTurboMQSource).name("turboMQSource")

   val ds = source.map(x=>{
      val id = x._1.split("_")(1).toInt
      val province =x._2.split("_")
      val address = province(province.length-1)
     val create_date = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS").format(new Date)
     //Info(id,address,create_date)
     (id,address)
    })
  // source.print()



    //添加table支持
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

    //创建table
    val sourcetable  = tEnv.fromDataStream(ds,'id,'address)

    val targetTable = "info"
    val sql = "insert into info (id, address) values(?, ?)"
    sourcetable.toAppendStream[Row].print()

    insertToTable(targetTable,sql,tEnv,sourcetable.getSchema,10)

    sourcetable.insertInto(targetTable)

    env.execute("test")

  }

  import org.apache.rocketmq.flink.RocketMQConfig
  import org.apache.rocketmq.flink.RocketMQSource

  private def builderTurboMQSource = {
    val consumerProps = new Properties()
    consumerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, "182.10.1.144:9876")
    consumerProps.setProperty(RocketMQConfig.CONSUMER_GROUP, "c006")
    consumerProps.setProperty(RocketMQConfig.CONSUMER_TOPIC, "flink-source2")
    val schema = new SimpleStringDeserializationSchema
    val turboMQConsumer = new RocketMQSource[(String,String)](schema, consumerProps)
    turboMQConsumer
  }
}
case class Info(id:Int,address:String,create_date:String)