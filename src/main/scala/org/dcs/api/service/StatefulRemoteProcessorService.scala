package org.dcs.api.service

import java.util.{List => JavaList, Map => JavaMap, Set => JavaSet}

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
                         properties: JavaList[RemoteProperty]): Boolean = get(processorStateId) match {
    case None => false
    case Some(p) => p.onSchedule(properties); true
  }

  def instanceExecute(processorStateId: String,
                      input: Array[Byte],
                      properties: JavaMap[String, String]): List[Either[ErrorResponse, AnyRef]] =
    get(processorStateId) match {
      case None => null
      case Some(p) => p.execute(input, properties)
    }

  def instanceTrigger(processorStateId: String,
                      input: Array[Byte],

                      properties: JavaMap[String, String]): JavaList[Either[Array[Byte], Array[Byte]]] =
    get(processorStateId) match {
      case None => null
      case Some(p) => p.trigger(input, properties)
    }

  def onInstanceUnschedule(processorStateId: String,
                           properties: JavaList[RemoteProperty]): Boolean = get(processorStateId) match {
    case None => false
    case Some(p) => p.onUnschedule(properties); true
  }

  def onInstanceStop(processorStateId: String,
                     properties: JavaList[RemoteProperty]): Boolean = get(processorStateId) match {
    case None => false
    case Some(p) => p.onStop(properties); true
  }

  def onInstanceShutdown(processorStateId: String,
                         properties: JavaList[RemoteProperty]): Boolean = get(processorStateId) match {
    case None => false
    case Some(p) => p.onShutdown(properties); true
  }

  def onInstanceRemove(processorStateId: String,
                       properties: JavaList[RemoteProperty]): Boolean = get(processorStateId) match {
    case None => false
    case Some(p) => {
      p.onRemove(properties)
      remove(processorStateId)
      true
    }
  }

}
