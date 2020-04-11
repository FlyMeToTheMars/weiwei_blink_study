package com.flink.utils

import java.sql.Types

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.io.jdbc.{JDBCAppendTableSink, JDBCOutputFormat}
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.flink.table.planner.sources.TableSourceUtil
import scala.collection.mutable.ListBuffer

object MysqlSink {


  /**
    * 获取mysqlsink连接
    */
  val getMysqlSink :(String,Array[TypeInformation[_]],Int)=>JDBCAppendTableSink = ( sql:String,types: Array[TypeInformation[_]],batchSize:Int)=>{

    val output =JDBCAppendTableSink.builder()
      .setDrivername(JdbcConfig.DRIVER_CLASS)
      .setDBUrl(JdbcConfig.SOURCE_DRIVER_URL)
      .setUsername(JdbcConfig.SOURCE_USER)
      .setPassword(JdbcConfig.PASS_WORD)
      .setParameterTypes(types:_*)
      .setBatchSize(batchSize)
      .setQuery(sql)
      .build()
    println("=====指标sink==="+sql)
    output
  }

  val getBatchMysqlSink :(String,TableSchema,Int)=>JDBCOutputFormat = ( sql:String,schema: TableSchema,batchSize:Int)=>{

    val output =JDBCOutputFormat.buildJDBCOutputFormat()
      .setDrivername(JdbcConfig.DRIVER_CLASS)
      .setDBUrl(JdbcConfig.SOURCE_DRIVER_URL)
      .setUsername(JdbcConfig.SOURCE_USER)
      .setPassword(JdbcConfig.PASS_WORD)
      .setBatchInterval(batchSize)
      .setQuery(sql)
      .setSqlTypes(getSqlTypes(schema))
      .finish()

    println("=====指标sink==="+sql)
    output
  }



  val insertToTable:(String,String,StreamTableEnvironment,TableSchema,Int)=>Unit =(tableName:String,sql:String,tableEnv:StreamTableEnvironment,schema:TableSchema,batchSize:Int)=>{
    println("=====数据库sink===")
    val output = getMysqlSink(sql,schema.getFieldTypes,batchSize)
    println(schema.getFieldTypes+"====")

    tableEnv.registerTableSink(tableName,schema.getFieldNames,schema.getFieldTypes,output)
    println(output.getFieldNames+"===")

  }

  val batchInsertToTable:(String,String,BatchTableEnvironment,TableSchema,Int)=>JDBCOutputFormat =(tableName:String,sql:String,tableEnv:BatchTableEnvironment,schema:TableSchema,batchSize:Int)=>{
    println("=====数据库sink===")
    val output = getBatchMysqlSink(sql,schema,batchSize)
    output
  }

  /**
    * 进行types格式转换
    */
  val getSqlTypes:(TableSchema)=>Array[Int] =(schema:TableSchema)=>{
   val int= schema.getFieldCount
    val sqlTypes = new ListBuffer[Int]
   // val schema :Array[Int] = null
    for(fieldType <- schema.getFieldTypes){
      sqlTypes.append(fieldType match {
        case BasicTypeInfo.INT_TYPE_INFO =>Types.INTEGER
        case BasicTypeInfo.STRING_TYPE_INFO=>Types.VARCHAR
        case BasicTypeInfo.DOUBLE_TYPE_INFO=>Types.DOUBLE
        case BasicTypeInfo.LONG_TYPE_INFO=>Types.BIGINT
        case BasicTypeInfo.FLOAT_TYPE_INFO=>Types.REAL
        case BasicTypeInfo.BYTE_TYPE_INFO=>Types.TINYINT
        case BasicTypeInfo.SHORT_TYPE_INFO=>Types.SMALLINT
        case BasicTypeInfo.BIG_INT_TYPE_INFO=>Types.BIGINT
      })
    }
    println( sqlTypes.toString)
    sqlTypes.toArray
  }



}
