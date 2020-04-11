package com.flink.demo.batchDemo

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
/**
  * author Renault
  * date 2019/12/9 9:54
  * Description 在批处理环境下使用表
  */
object wordCountWithBatchTable {
  def main(args: Array[String]): Unit = {
    /**
      * 构建环境
      */
    val env = ExecutionEnvironment.getExecutionEnvironment

    /**
      * 获取数据
      */
    val words = env.readTextFile("data/word.txt")
      .flatMap(_.split(",")).map((_,1))

    /**
      * 添加表环境
      */
    val filedname = "word,num"
    val tableEnv = BatchTableEnvironment.create(env)
    val wordtable =tableEnv.fromDataSet(words,'word,'num)
    wordtable.groupBy("word").select('word, 'num.count as 'cnt)
      .toDataSet[Row] // conversion to DataSet
      .print()
//    tableEnv.registerDataSet("times",words,filedname)
//    tableEnv.sqlQuery("select * from times limit 1").toDataSet[Row].print()

    /**
      * wordCount With SQL
      * 如果你的表没有被注册既register 则需要使用$tablename 作为引用添加到语句中
      */
    //val ds = words.toTable(tableEnv,'word,'num)
  val result =  tableEnv.sqlQuery(s"select word,sum(num) from $wordtable group by word ")
    result.toDataSet[Row].print()

    /**
      * 如果你的表被注册
      * 则直接使用表名完成搜索及查询
      */
        tableEnv.registerDataSet("Orders", words, 'key, 'cnt)
        tableEnv.scan("Orders")
        tableEnv.sqlQuery("select key,sum(cnt) from Orders group by key").toDataSet[Row].print()

  }

}
