package com.flink.utils

import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat.JDBCInputFormatBuilder
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.mutable.ListBuffer
import org.apache.flink.api.scala._
import com.flink.utils.JdbcConfig._
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.types.Row
object MysqlSource {

    //使用jdbcinput的方式获取mysql 数据源
  val getMysqlSource =(env:StreamExecutionEnvironment,size:Int,sql:String)=>{

  val input = JDBCInputFormat
    .buildJDBCInputFormat()
    .setDBUrl(SOURCE_DRIVER_URL)
    .setDrivername(JdbcConfig.DRIVER_CLASS)
    .setUsername(JdbcConfig.SOURCE_USER)
    .setPassword(JdbcConfig.PASS_WORD)
    .setFetchSize(size)
    .setQuery(sql)
    .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO))
    .finish()
  env.createInput(input)
}

  val getRunningCmdbDs:(StreamExecutionEnvironment)=>DataStream[(String,String,Double,String,String,String)]= (env:StreamExecutionEnvironment)=>{
    val cmdbDS=  env.addSource(new CmdbReader()).filter(_._1.nonEmpty).filter(_._2.nonEmpty)
    val cmdbs= cmdbDS
      .flatMap(x=>{
        val pro_ip = x._1.split("\\;")
        val pro_type= x._2
        var serv_capacity = 0.0
        if(x._3.nonEmpty){
          serv_capacity = x._3.toDouble
        }
        val busi_grpname= x._4
        val soft_factory = x._5
        val city = x._6
        val list = new ListBuffer[String]
        for( e <- pro_ip){
          list.append(x._2+"_"+e+"_"+serv_capacity+"_"+busi_grpname+"_"+soft_factory+"_"+city)
        }
        list
      }).map(x=>{
      val split = x.split("\\_")
      (split(0),split(1),split(2).toDouble,split(3),split(4),split(5))
    })
    cmdbs
  }

//  val getRedominDs:(StreamExecutionEnvironment)=>DataStream[(String,String)]= (env:StreamExecutionEnvironment)=>{
//    val redominDS=  env.addSource(new MysqlRedominReader()).filter(_._1.nonEmpty).filter(_._2.nonEmpty)
//   val result = redominDS.map(x=>{
//      (x._1,x._2)
//    })
//    result
//  }

}
case class Cmdb(pro_ipv4:String,pro_type:String,serv_capacity:Double,busi_grpname:String,soft_factory:String,city:String)
