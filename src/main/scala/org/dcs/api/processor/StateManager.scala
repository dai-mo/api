package org.dcs.api.processor

import java.util.{List => JavaList}

/**
  * Created by cmathew on 06/09/16.
  */
trait StateManager {

  def put(processor: StatefulRemoteProcessor): String

  def get(processorStateId: String): Option[StatefulRemoteProcessor]

  def remove(processorStateId: String): Boolean

}
