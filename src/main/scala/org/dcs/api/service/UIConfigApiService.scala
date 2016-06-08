package org.dcs.api.service

import scala.beans.BeanProperty

/**
  * Created by cmathew on 05/06/16.
  */

case class UIConfig(@BeanProperty var nifiUrl: String) {
  def this() = this("")
}

trait UIConfigApiService {
  def config(): UIConfig
}
