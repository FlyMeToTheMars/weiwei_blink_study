package com.flink.sinkToClickhouse

import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._


object oracelToClickhouse {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("checkData").master("local[6]")
      //.config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport().getOrCreate()

    //jdbc读取oracel


    //csv写入
    val table = args(0)
//     val df =      spark.read .option("header", "true") //在csv第一行有属性"true"，没有就是"false"
//      .option("inferSchema", true) //这是自动推断属性列的数据类型
//      .option("delimiter", ",")
//      .csv(s"E:\\work\\5g网络孤帆\\pgsql调研\\第三批\\gz-5g-data\\$table.csv")
    val df = spark.read.format("jdbc")
      .options(Map("url" -> "jdbc:oracle:thin:@192.168.52.125:1521:orcl",
        "driver" -> "oracle.jdbc.driver.OracleDriver",
        "dbtable" -> s"HA_RESMANAGE.$table",
        "user" -> "NGNMP",
        "password" -> "NGNMP_2018")).load()
//    df.write
//    .mode(SaveMode.Overwrite)
//      .parquet("hdfs://node-143/user/HA_RESMANAGE/CE_DEVICE_PON")
       val pro= new Properties()
        pro.put("user","postgres")
        pro.put("password","postgres")
        pro.put("driver","org.postgresql.Driver")
//    val orapro =new Properties()
//    orapro.put("user","NGNMP")
//    orapro.put("password","NGNMP_2018")
//    orapro.put("driver","oracle.jdbc.driver.OracleDriver")
//    orapro.put("database","HA_RESMANAGE")

//       val pgdf = spark.read
//         .jdbc("jdbc:postgresql://182.10.1.171:5432/postgres","table_name",pro)
//
//     pgdf.show()

//df.printSchema()
   val cols = df.columns.map(x => col(x.toLowerCase))

//    df
////      .withColumn("create_date",lit("2010-02-10 00:00:00").cast("timestamp"))
//
//
    df.select(cols: _*)
      .write
          .mode(SaveMode.Overwrite)
      .jdbc("jdbc:postgresql://192.168.52.77:5432/postgres",table,pro)
   // df.write.mode(SaveMode.Append).jdbc("jdbc:oracle:thin:@192.168.52.125:1521:orcl","CM_LINK_DE",orapro)



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
