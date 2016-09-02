package org.dcs.api.processor

import scala.beans.BeanProperty

/**
  * Created by cmathew on 31/08/16.
  */
trait HasConfiguration {

  def configuration(): Configuration

}

case class Configuration(@BeanProperty var inputMimeType: String,
                         @BeanProperty var outputMimeType: String) {
  def this() = this("", "")
}
