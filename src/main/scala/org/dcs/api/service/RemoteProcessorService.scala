package org.dcs.api.service

import java.util.{List => JavaList, Map => JavaMap, Set => JavaSet}

import org.apache.avro.Schema
import org.dcs.api.processor._
import org.dcs.commons.error.ErrorResponse

/**
  * Created by cmathew on 06/09/16.
  */

trait RemoteProcessorService extends RemoteProcessor
  with DefinitionStore
  with StatelessProcessor {


  override def execute(input: Array[Byte], properties: JavaMap[String, String]): List[Either[ErrorResponse, AnyRef]] = {
    instance().execute(input, properties)
  }

  override def trigger(input: Array[Byte], properties: JavaMap[String, String]): JavaList[Either[Array[Byte], Array[Byte]]] = {
    instance().trigger(input, properties)
  }

  override def properties(): JavaList[RemoteProperty] =
    getDef(instance()).properties()


  override def propertyValue(propertySettings: RemoteProperty,
                             values: JavaMap[String, String]): String =
    getDef(instance()).propertyValue(propertySettings, values)

  override def relationships(): JavaSet[RemoteRelationship] =
    getDef(instance()).relationships()

  override  def configuration: Configuration =
    getDef(instance()).configuration

  override def metadata():MetaData =
    getDef(instance()).metadata()

  override def schema(): Option[Schema] =
    instance().schema()
}


trait DefinitionStore {
  def getDef(processor: RemoteProcessor): ProcessorDefinition
}