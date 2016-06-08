package org.dcs.api.service;

import java.util.{Map => JavaMap}

trait ModuleFactoryService {

  def createFlowModule(className: String): String

  def getPropertyDescriptors(moduleUUID: String): JavaMap[String, JavaMap[String, String]]

  def getRelationships(moduleUUID: String): JavaMap[String, JavaMap[String, String]]

  def schedule(moduleUUID: String): Boolean

  def trigger( moduleUUID: String, properties: JavaMap[String, String]): Array[Byte]

  def unschedule(moduleUUID: String): Boolean

  def stop(moduleUUID: String): Boolean

  def shutdown(moduleUUID: String): Boolean

  def remove(moduleUUID: String): Boolean

}