package org.dcs.api.processor

import java.util.{Map => JavaMap}


trait RemoteProcessor extends BaseProcessor
	with HasProperties
	with HasRelationships
  with HasConfiguration {

	def schedule(): Boolean

  def execute(input: Array[Byte], properties: JavaMap[String, String]): AnyRef

	def trigger(input: Array[Byte], properties: JavaMap[String, String]): Array[Byte]

	def unschedule(): Boolean

	def stop(): Boolean

	def shutdown(): Boolean

	def remove(): Boolean

}
