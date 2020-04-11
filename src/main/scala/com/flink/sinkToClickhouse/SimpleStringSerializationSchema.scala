
package com.flink.sinkToClickhouse

import java.nio.charset.StandardCharsets

import breeze.linalg.Axis._1
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.rocketmq.flink.common.serialization.KeyValueSerializationSchema

class SimpleStringSerializationSchema extends  KeyValueSerializationSchema[(String,String)] {
  val DEFAULT_KEY_FIELD = "key"
  val DEFAULT_VALUE_FIELD = "value"

  var keyField: String = null
  var valueField: String = null


  /**
    * SimpleKeyValueSerializationSchema Constructor.
    *
    * @param keyField   tuple field for selecting the key
    * @param valueField tuple field for selecting the value
    */
  def this(keyField: String, valueField: String) {
    this()
    this.keyField = keyField
    this.valueField = valueField
  }

  override def serializeKey(tuple: (String,String)): Array[Byte] = {
    if (tuple == null || keyField == null) return null
    val key = tuple._1
    if (key != null) key.toString.getBytes(StandardCharsets.UTF_8) else null
  }

  override def serializeValue(tuple: (String,String)): Array[Byte] = {
    if (tuple == null || valueField == null) return null
    val value = tuple._2
    if (value != null) value.toString.getBytes(StandardCharsets.UTF_8) else null
  }

}
