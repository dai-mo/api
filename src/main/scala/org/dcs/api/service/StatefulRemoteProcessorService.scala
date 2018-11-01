/*
 * Copyright (c) 2017-2018 brewlabs SAS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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
