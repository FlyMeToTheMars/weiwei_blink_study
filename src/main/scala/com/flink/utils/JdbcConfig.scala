package com.flink.utils
import  com.flink.utils.PropertiesScalaUtils._
object JdbcConfig {

  val GROK_PATTERN_PATH  = "E:\\BIGDATE\\cdn_parse\\src\\main\\data\\patterns"

  /**
    * JDBC连接
    */
  val DRIVER_CLASS =loadProperties.getProperty("driver_class")
  val SOURCE_DRIVER_URL =loadProperties.getProperty("source_driver_url")
  val SOURCE_USER = loadProperties.getProperty("source_user")
  val PASS_WORD = loadProperties.getProperty("pass_word")


}
