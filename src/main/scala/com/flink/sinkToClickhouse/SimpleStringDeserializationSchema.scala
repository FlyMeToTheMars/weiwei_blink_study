
package com.flink.sinkToClickhouse

import org.apache.rocketmq.flink.common.serialization.KeyValueDeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation

class SimpleStringDeserializationSchema extends  KeyValueDeserializationSchema[(String,String)] {
  private val serialVersionUID = 1L
  override def deserializeKeyAndValue(key: Array[Byte], value: Array[Byte]): (String,String) = {
    import java.nio.charset.StandardCharsets
    val k = if (key != null) new String(key, StandardCharsets.UTF_8)    else ""

    val v = if (value != null) new String(value, StandardCharsets.UTF_8)
    else ""
    (k,v)
  }

  override def getProducedType: TypeInformation[(String,String)] = {
    return TypeInformation.of(classOf[(String,String)])
  }
}
