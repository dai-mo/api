package org.dcs.api.processor

import java.util.{List => JavaList, Map => JavaMap}

import org.apache.avro.Schema
import org.dcs.commons.error.ErrorResponse
import org.dcs.commons.serde.AvroImplicits._

trait RemoteProcessor extends BaseProcessor
  with ProcessorDefinition {

  def execute(input: Array[Byte], properties: JavaMap[String, String]): List[Either[ErrorResponse, AnyRef]]

  def trigger(input: Array[Byte], properties: JavaMap[String, String]): Array[Array[Byte]] = {

    execute(input, properties).map{ out =>
      if (out.isLeft)
        out.left.get.serToBytes()
      else {
        out.right.get.serToBytes(schema)
      }
    }.toArray
  }

  def schema(): Option[Schema]
}

trait ProcessorDefinition extends HasProperties
  with HasRelationships
  with HasConfiguration
  with HasMetaData


