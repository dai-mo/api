package org.dcs.api.processor

import java.util.{List => JavaList, Map => JavaMap}


trait RemoteProcessor extends BaseProcessor
  with ProcessorDefinition {

  def execute(input: Array[Byte], properties: JavaMap[String, String]): AnyRef
  def trigger(input: Array[Byte], properties: JavaMap[String, String]): Array[Byte]
}

trait ProcessorDefinition extends HasProperties
  with HasRelationships
  with HasConfiguration
  with HasMetaData


