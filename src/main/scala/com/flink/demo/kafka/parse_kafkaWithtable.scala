package com.flink.demo.kafka

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import com.flink.utils.PropertiesScalaUtils._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Over}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala._

/**
  * author Renault
  * date 2019/12/13 15:37
  * Description 
  */
object parse_kafkaWithTable {
  def main(args: Array[String]): Unit = {
    //获取流处理环境
     val env = StreamExecutionEnvironment.getExecutionEnvironment

    //注册时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    //设置checkpoint
    //env.enableCheckpointing(20000, CheckpointingMode.AT_LEAST_ONCE) //kafka数据

    //添加kafka支持

    val topic = "filebeat_topic"
    val kafkaProps = new Properties()
   // kafkaProps.setProperty("zookeeper.connect",loadProperties.getProperty("zk_port"))
    kafkaProps.setProperty("bootstrap.servers",loadProperties.getProperty("bootstrap.servers"))
    kafkaProps.setProperty("group.id", "test")

    val consumer = new FlinkKafkaConsumer[String](
      topic,
      new SimpleStringSchema(),
      kafkaProps)
    consumer.setStartFromEarliest()
    val kafkaSource = env.addSource(consumer)

    val source = kafkaSource.map(lines=>{
      val json = JSON.parseObject(lines)
      val message = json.getString("message")
      val messagesplit = message.split("\\|")
      val id = messagesplit(0).toInt
      val job = messagesplit(1)
      val email = messagesplit(2)
      val name = messagesplit(3)
      val age = messagesplit(4).toInt
      val area = messagesplit(5)
      Person(id,job,email,name,age,area)
    })
    //添加table支持
    //val tableenv =StreamTableEnvironment.create(env)
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val bsTableEnv = StreamTableEnvironment.create(env, bsSettings);
    // or TableEnvironment bsTableEnv = TableEnvironment.create(bsSettings);
    val table= bsTableEnv.fromDataStream(source,'id, 'Job, 'email, 'name, 'age, 'area,'UserActionTime.proctime)
  // val result =    bsTableEnv.sqlQuery(s"select  area,LAST_VALUE(count(1)) from $table  group by area".stripMargin)


    table.window(Over partitionBy 'area orderBy 'UserActionTime  preceding 2.rows following CURRENT_ROW as 'w)
        .select('area, 'age, 'age.count over 'w)
    .toRetractStream[Row].print()

    //  kafkaSource.print()
    env.execute("testjob")

  }

}
case class Person(id:Int,Job:String,email:String,name:String,age:Int,area:String)
