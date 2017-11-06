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

  override def execute(record: Option[GenericRecord], properties: JavaMap[String, String]): List[Either[ErrorResponse, (String, AnyRef)]] = {
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

  override def processorType(): String = instance().processorType

  override  def configuration: Configuration =
    getDef(instance()).configuration

  override def metadata():MetaData =
    getDef(instance()).metadata()

  override def details(): ProcessorDetails =
    getDef(instance()).details()

  override def resolveProperties(properties: util.Map[String, String]): util.Map[String, String] =
    instance().resolveProperties(properties)

  override def schemaId: String =
    instance().schemaId

  override def preStart(properties: util.Map[String, String]): Boolean =
    instance().preStart(properties)

  override def preStop(properties: util.Map[String, String]): Boolean =
    instance().preStop(properties)

  override def postRemove(properties: util.Map[String, String]): Boolean =
    instance().postRemove(properties)
}


trait DefinitionStore {
  def getDef(processor: RemoteProcessor): ProcessorDefinition
}