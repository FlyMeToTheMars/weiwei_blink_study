package com.flink.sinkToClickhouse

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * author Renault
  * date 2020/1/7 14:18
  * Description 
  */
object csvtomysql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("checkData").master("local[4]")
      //.config("spark.sql.warehouse.dir", warehouseLocation)
     // .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

   val df = spark.read
      .option("header", "true") //在csv第一行有属性"true"，没有就是"false"
      .option("inferSchema", true) //这是自动推断属性列的数据类型
      .option("delimiter", ",")
      .csv("E:\\data-mysql2\\ce_link_medium.csv")


    val pro= new Properties()
    pro.put("user","root")
    pro.put("password","123456")
    pro.put("driver","com.mysql.jdbc.Driver")

    df
      .write
      .mode(SaveMode.Append)
      .jdbc("jdbc:mysql://182.10.1.173:3306/gz_cloud_resources?useUnicode=true&characterEncoding=utf-8&rewriteBatchedStatements=true","ce_link_medium",pro)
  }
}
