package org.dcs.api.service

import java.util.{List => JavaList, Map => JavaMap, Set => JavaSet}

import org.apache.avro.generic.GenericRecord
import org.dcs.api.processor._
import org.dcs.commons.error.ErrorResponse

/**
  * Created by cmathew on 06/09/16.
  */
trait StatefulRemoteProcessorService extends RemoteProcessorService  {
  this: StateManager =>

  def init(): String

  def onInstanceConfigurationRestore(processorStateId: String): Boolean  = get(processorStateId) match {
    case None => false
    case Some(p) => p.onConfigurationRestore(); true
  }

  def onInstancePropertyChanged(processorStateId: String,
                                property: RemoteProperty): Boolean = get(processorStateId) match {
    case None => false
    case Some(p) => p.onPropertyChanged(property); true
  }

  def onInstanceAdd(processorStateId: String): Boolean = get(processorStateId) match {
    case None => false
    case Some(p) =>  p.onAdd(); true
  }

  def onInstanceSchedule(processorStateId: String,
                         propertyValues: JavaMap[RemoteProperty, String]): Boolean = get(processorStateId) match {
    case None => false
    case Some(p) => p.onSchedule(propertyValues); true
  }

  def instanceExecute(processorStateId: String,
                      record: Option[GenericRecord],
                      properties: JavaMap[String, String]): List[Either[ErrorResponse, AnyRef]] =
    get(processorStateId) match {
      case None => null
      case Some(p) => p.execute(record, properties)
    }

  def instanceTrigger(processorStateId: String,
                      input: Array[Byte],
                      properties: JavaMap[String, String]): Array[Array[Byte]] =
    get(processorStateId) match {
      case None => null
      case Some(p) => p.trigger(input, properties)
    }

  def onInstanceUnschedule(processorStateId: String,
                           propertyValues: JavaMap[RemoteProperty, String]): Boolean = get(processorStateId) match {
    case None => false
    case Some(p) => p.onUnschedule(propertyValues); true
  }

  def onInstanceStop(processorStateId: String,
                     propertyValues: JavaMap[RemoteProperty, String]): Boolean = get(processorStateId) match {
    case None => false
    case Some(p) => p.onStop(propertyValues); true
  }

  def onInstanceShutdown(processorStateId: String,
                         propertyValues: JavaMap[RemoteProperty, String]): Boolean = get(processorStateId) match {
    case None => false
    case Some(p) => p.onShutdown(propertyValues); true
  }

  def onInstanceRemove(processorStateId: String,
                       propertyValues: JavaMap[RemoteProperty, String]): Boolean = get(processorStateId) match {
    case None => false
    case Some(p) => {
      p.onRemove(propertyValues)
      remove(processorStateId)
      true
    }
  }

}
