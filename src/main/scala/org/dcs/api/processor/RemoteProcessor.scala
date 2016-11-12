package org.dcs.api.processor

import java.util
import java.util.{List => JavaList, Map => JavaMap}

import org.dcs.commons.error.ErrorResponse
import org.dcs.commons.serde.AvroImplicits._

import org.apache.avro.Schema

trait RemoteProcessor extends BaseProcessor
  with ProcessorDefinition {

  def execute(input: Array[Byte], properties: JavaMap[String, String]): List[Either[ErrorResponse, AnyRef]]

  def trigger(input: Array[Byte], properties: JavaMap[String, String]): JavaList[Either[Array[Byte], Array[Byte]]] = {
    val result = new util.ArrayList[Either[Array[Byte], Array[Byte]]]()
    execute(input, properties).foreach{ out =>
      result.add (
        if (out.isLeft)
          Left(out.left.get.serToBytes())
        else {
          Right(out.right.get.serToBytes(schema))
        }
      )
    }
    result
  }

  def schema(): Option[Schema]
}

trait ProcessorDefinition extends HasProperties
  with HasRelationships
  with HasConfiguration
  with HasMetaData


