import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
/**
  * author Renault
  * date 2020/1/3 10:43
  * Description 
  */
object namettse {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //测试数据
    val words = env.readTextFile("data/word.txt")
      .flatMap(_.split(",")).map((_,1))      .map(x=>(info(x._1,x._2)))

    val tableEnv = BatchTableEnvironment.create(env)
    val map = Map("id"->1,"name"->"zhang")
    val filed = map.keys.mkString(",")
    println(filed)

  tableEnv.registerDataSet("t",words,"id, name")
    tableEnv.sqlQuery("select id,count(name) from t group by id")
      .toDataSet[Row] // conversion to DataSet
      .print()

    env.execute("name")

  }
  case class info(id:String,name:Int)

}
