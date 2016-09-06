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

  def onSchedule(properties: JavaList[RemoteProperty]): Boolean = true

  def onUnschedule(properties: JavaList[RemoteProperty]): Boolean = true

  def onStop(properties: JavaList[RemoteProperty]): Boolean = true

  def onShutdown(properties: JavaList[RemoteProperty]): Boolean = true

  def onRemove(properties: JavaList[RemoteProperty]): Boolean = true

}
