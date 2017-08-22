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
