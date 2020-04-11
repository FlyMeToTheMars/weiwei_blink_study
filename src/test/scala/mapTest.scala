import org.apache.spark.sql.SparkSession

/**
  * author Renault
  * date 2020/1/3 10:38
  * Description 
  */
object mapTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("checkData").master("local[4]")
      //.config("spark.sql.warehouse.dir", warehouseLocation)
     .getOrCreate()

    val map = Map("id"->1,"name"->"李四")

    val clume  = map.keys.toList.mkString(",")
    println(clume)


  }

}
