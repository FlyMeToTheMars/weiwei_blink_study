package com.flink.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.slf4j.{Logger, LoggerFactory}
import com.flink.utils.PropertiesScalaUtils._
/**
  * 使用jdbc连接
  * 获取mysql数据
  */
class CmdbReader extends RichSourceFunction[(String,String,String,String,String,String)]{

  final val logger: Logger = LoggerFactory.getLogger(CmdbReader.this.getClass)

  private var conn:Connection=null
  private var ps:PreparedStatement=null
  private var isRunning:Boolean = true


  override def open(parameters: Configuration): Unit ={
    System.out.println("建立连接")
    Class.forName(JdbcConfig.DRIVER_CLASS)
    conn = DriverManager.getConnection(JdbcConfig.SOURCE_DRIVER_URL,JdbcConfig.SOURCE_USER,JdbcConfig.PASS_WORD)//获取连接
   val table = loadProperties.getProperty("cmdb")
    val sql = s"select * from $table"

    ps = conn.prepareStatement(sql)

  }

  //获取
  override def run(sourceContext: SourceFunction.SourceContext[(String,String,String,String,String,String)]): Unit = {
    while(isRunning ){
      //获取cmdb静态数据
      try {
        val resultSet = ps.executeQuery()
        while (resultSet.next()) {
          val pro_ipv4 = resultSet.getString("BUSI_IPV4") //业务ip
          val pro_type = resultSet.getString("BEAR_BUSI")  //业务类型
          val serv_capacity = resultSet.getString("SERV_CAPACITY") //服务能力
          val busi_grpname = resultSet.getString("BUSI_GRPNAME") //业务分组名
          val soft_factory = resultSet.getString("SOFT_FACTORY")  //厂商
          val city = resultSet.getString("REGION")  //地市
          sourceContext.collect((pro_ipv4, pro_type,serv_capacity,busi_grpname,soft_factory,city))
        }
      } catch {
        case e: Exception => println(e)
      }
      Thread.sleep(1000 * 10 * 6 * 60 * 24 )
    }
  }

  override def cancel(): Unit = {
    if(conn!=null){
      conn.close()
    }
    if(ps!=null){
      ps.close()
    }
    isRunning = false

  }
}
