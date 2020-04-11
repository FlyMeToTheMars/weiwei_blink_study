package com.flink.sinkToClickhouse

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}


object oracelTopg {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("checkData").master("local[1]")
      //.config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport().getOrCreate()

//    val tablename = args(0).toUpperCase
//    val table = args(0)
    //jdbc读取oracel
    val df = spark.read.format("jdbc")
      .options(Map("url" -> "jdbc:oracle:thin:@192.168.52.125:1521:orcl",
        "driver" -> "oracle.jdbc.driver.OracleDriver",
        "dbtable" -> s"tabs",
        "user" -> "NGNMP",
        "password" -> "NGNMP_2018")).load()

    df.select("table_name").show()
   // df.selectExpr("count(table_name)").show()
    //csv写入
//     val df =      spark.read .option("header", "true") //在csv第一行有属性"true"，没有就是"false"
//      .option("inferSchema", true) //这是自动推断属性列的数据类型
//      .option("delimiter", ",")
//      .csv("E:\\work\\5g网络孤帆\\gz-5g-data\\cm_link.csv")

//    df.write
//    .mode(SaveMode.Overwrite)
//      .parquet("hdfs://node-143/user/HA_RESMANAGE/CE_DEVICE_PON")
       val pro= new Properties()
        pro.put("user","postgres")
        pro.put("password","postgres")
        pro.put("driver","org.postgresql.Driver")

//       val pgdf = spark.read
//         .jdbc("jdbc:postgresql://182.10.1.171:5432/postgres","table_name",pro)
//
//     pgdf.show()
//    df
//          .write
//          .mode(SaveMode.Append)
//            .jdbc("jdbc:postgresql://192.168.52.235:9999/postgres",table,pro)

    //          .option("batchsize", "50000")
//          .option("isolationLevel", "NONE") // 设置事务
//          .option("numPartitions", "1") // 设置并发
//          .jdbc(dbUrl,
//          "table",
//          dbProp)


//    val ckdf = spark.read.format("jdbc")
//      .options(Map("url" -> "jdbc:clickhouse://182.10.1.144:9001",
//        "driver" -> "com.github.housepower.jdbc.ClickHouseDriver",
//        "dbtable" -> "default.info",
//        "user" -> "default")
//      ).load()
//    ckdf.show()
  }

}
