package com.flink.utils

import java.util.Properties

/**
  * @author Wang.long
  * @date 2019/8/27 14:33
  * @Description * @Mail wang.long@ustcinfo.com
  */
object PropertiesScalaUtils {

  def main(args: Array[String]): Unit = {
    val pro = loadProperties
    println(pro.getProperty("driver_class"))
    println(pro.getProperty("source_driver_url"))

  }
  def loadProperties: Properties =  {
    val properties = new Properties()
    val path = PropertiesScalaUtils.getClass.getClassLoader.getResourceAsStream("logParsing.properties")
  //  val path = Thread.currentThread().getContextClassLoader.getResource("logParsing.properties").getPath //文件要放到resource文件夹下
    properties.load(path)
    properties
  }

}
