/**
  * author Renault
  * date 2019/12/18 10:47
  * Description 
  */
import javax.xml.validation.Schema
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.types.Row

import scala.collection.JavaConversions._
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{DataTypes, TableSchema}

object typeChange {
  def main(args: Array[String]): Unit = {
    /**
      * 尝试flink中的 类型转换
      *
      */
    //设置一个类
    val schema = new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)


    val schema1 = "{\"type\":\"record\",\"name\":\"tablename\",\"fields\":[{\"name\":\"c_1\",\"type\":\"int\"},{\"name\":\"c_2\",\"type\":\"int\"},{\"name\":\"c_ascode\",\"type\":\"int\"},{\"name\":\"c_1\",\"type\":\"string\"}]}"
    val filedList = Array[String]()
    val tableschema: TableSchema = TableSchema.builder()
      .field("id", DataTypes.STRING())
      .field("ip", DataTypes.STRING())
      .field("port", DataTypes.INT())
      .field("alias", DataTypes.STRING())
      .field("connect_time", DataTypes.DATE())
      .field("disconnect_time", DataTypes.DATE())
      .build()
    val fieldTypes = Array[TypeInformation[_]](BasicTypeInfo.INT_TYPE_INFO,BasicTypeInfo.BIG_INT_TYPE_INFO)

    val rowTypeInfo = new RowTypeInfo(fieldTypes:_*)
     println(rowTypeInfo.getFieldTypes.toList)

//    private lazy val physicalRowFieldTypes: Seq[TypeInformation[_]] =
//    logicalRowType.getFieldList map { f => FlinkTypeFactory.toTypeInfo(f.getType) }
//
//    private lazy val physicalRowTypeInfo: TypeInformation[Row] = new RowTypeInfo(
//      physicalRowFieldTypes.toArray, fieldNames.toArray)

  }

}
