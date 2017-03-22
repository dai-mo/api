package org.dcs.api.service

import java.util
import java.util.{List => JavaList, Map => JavaMap, Set => JavaSet}

import org.apache.avro.generic.GenericRecord
import org.dcs.api.processor._
import org.dcs.commons.error.ErrorResponse

/**
  * Created by cmathew on 06/09/16.
  */

trait RemoteProcessorService extends RemoteProcessor
  with DefinitionStore
  with StatelessProcessor {

  override def execute(record: Option[GenericRecord], properties: JavaMap[String, String]): List[Either[ErrorResponse, AnyRef]] = {
    instance().execute(record, properties)
  }

  override def trigger(input: Array[Byte], properties: JavaMap[String, String]): Array[Array[Byte]] = {
    instance().trigger(input, properties)
  }

  override def className: String = instance().className

  override def relationships(): util.Set[RemoteRelationship] = getDef(instance()).relationships()

  override def properties(): util.List[RemoteProperty] = getDef(instance()).properties()

  override def propertyValue(propertySettings: RemoteProperty,
                             values: JavaMap[String, String]): String =
    getDef(instance()).propertyValue(propertySettings, values)

  override def processorType(): String = instance().processorType()

  override  def configuration: Configuration =
    getDef(instance()).configuration

  override def metadata():MetaData =
    getDef(instance()).metadata()

  override def schemaId: String =
    instance().schemaId
}


trait DefinitionStore {
  def getDef(processor: RemoteProcessor): ProcessorDefinition
}