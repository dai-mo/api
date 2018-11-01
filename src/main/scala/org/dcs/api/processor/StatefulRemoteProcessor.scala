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

package org.dcs.api.processor

import java.util.{UUID, List => JavaList, Map => JavaMap}

/**
  * Created by cmathew on 06/09/16.
  */
trait StatefulRemoteProcessor extends RemoteProcessor {

  def init(stateManager: StateManager): String = {
    this.initState()
    stateManager.put(this)
  }

  def initState(): Unit

  def onConfigurationRestore(): Boolean = true

  def onPropertyChanged(property: RemoteProperty): Boolean = true

  def onAdd(): Boolean = true

  def onSchedule(propertyValues: JavaMap[RemoteProperty, String]): Boolean = true

  def onUnschedule(propertyValues: JavaMap[RemoteProperty, String]): Boolean = true

  def onStop(propertyValues: JavaMap[RemoteProperty, String]): Boolean = true

  def onShutdown(propertyValues: JavaMap[RemoteProperty, String]): Boolean = true

  def onRemove(propertyValues: JavaMap[RemoteProperty, String]): Boolean = true

}
