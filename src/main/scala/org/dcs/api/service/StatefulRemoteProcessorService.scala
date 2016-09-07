package org.dcs.api.service

import java.util.{List => JavaList, Map => JavaMap, Set => JavaSet}

import org.dcs.api.processor._

/**
  * Created by cmathew on 06/09/16.
  */
trait StatefulRemoteProcessorService extends RemoteProcessorService  {
  this: StateManager =>

  def init(): String

  def onConfigurationRestore(processorStateId: String): Boolean  = get(processorStateId) match {
    case None => false
    case Some(p) => p.onConfigurationRestore(); true
  }

  def onPropertyChanged(processorStateId: String,
                        property: RemoteProperty): Boolean = get(processorStateId) match {
    case None => false
    case Some(p) => p.onPropertyChanged(property); true
  }

  def onAdd(processorStateId: String): Unit = get(processorStateId) match {
    case None => Unit
    case Some(p) =>  p.onAdd()
  }

  def onSchedule(processorStateId: String,
                 properties: JavaList[RemoteProperty]): Boolean = get(processorStateId) match {
    case None => false
    case Some(p) => p.onSchedule(properties); true
  }

  def execute(processorStateId: String,
              input: Array[Byte],
              properties: JavaMap[String, String]): AnyRef = get(processorStateId) match {
    case None => null
    case Some(p) => p.execute(input, properties)
  }

  def trigger(processorStateId: String,
              input: Array[Byte],
              properties: JavaMap[String, String]): Array[Byte] = get(processorStateId) match {
    case None => null
    case Some(p) => p.trigger(input, properties)
  }

  def onUnschedule(processorStateId: String,
                   properties: JavaList[RemoteProperty]): Boolean = get(processorStateId) match {
    case None => false
    case Some(p) => p.onUnschedule(properties); true
  }

  def onStop(processorStateId: String,
             properties: JavaList[RemoteProperty]): Boolean = get(processorStateId) match {
    case None => false
    case Some(p) => p.onStop(properties); true
  }

  def onShutdown(processorStateId: String,
                 properties: JavaList[RemoteProperty]): Boolean = get(processorStateId) match {
    case None => false
    case Some(p) => p.onShutdown(properties); true
  }

  def onRemove(processorStateId: String,
               properties: JavaList[RemoteProperty]): Boolean = get(processorStateId) match {
    case None => false
    case Some(p) => {
      p.onRemove(properties)
      remove(processorStateId)
      true
    }
  }

}
