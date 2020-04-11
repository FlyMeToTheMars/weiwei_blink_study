package com.flink.sinkToClickhouse

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

/**
  * author Renault
  * date 2020/3/11 14:08
  * Description 
  */
object matedataToPg {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("pgtopg")
      //.config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport().getOrCreate()

    //    val tablename = args(0).toUpperCase
    //    val table = args(0)
    //jdbc读取postgres
    val df = spark.read.format("jdbc")
      .options(Map("url" -> "jdbc:postgresql://192.168.52.77:5432/metadata",
        "driver" -> "org.postgresql.Driver",
        "dbtable"->"pg_tables",
        "user" -> "postgres",
        "password" -> "postgres")).load()

    val pro= new Properties()
    pro.put("user","postgres")
    pro.put("password","postgres")
    pro.put("driver","org.postgresql.Driver")


    df.select("tablename").where("schemaname = 'public'")
      .collect().toList
      .foreach(x=>{
        val tablename = x.getString(0)
        println(s"====table $tablename 开始导入====")
        val orgtable = spark.read.format("jdbc")
          .options(Map("url" -> "jdbc:postgresql://192.168.52.77:5432/metadata",
            "driver" -> "org.postgresql.Driver",
            "dbtable"->s"$tablename",
            "user" -> "postgres",
            "password" -> "postgres")).load()
        val cols = orgtable.columns.map(x => col(x.toLowerCase))

        try{
          orgtable.select(cols: _*)
            .write
            .mode(SaveMode.Ignore)
            .jdbc("jdbc:postgresql://192.168.52.71:5432/metadata",tablename,pro)
          println(s"====table $tablename 导入完成====")

        }catch {
          case ex: Exception =>
            println(s"====table $tablename 导入失败====")
        }
      })


  }

}
