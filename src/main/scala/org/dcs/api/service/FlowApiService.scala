package org.dcs.api.service

import scala.beans.BeanProperty

/**
  * Created by cmathew on 05/06/16.
  */

case class Processor(@BeanProperty var pType: String,
                     @BeanProperty var description: String,
                     @BeanProperty var tags: List[String]) {
  def this() = this("", "", Nil)
}

trait FlowApiService {
  def processorTypes(tagFilter: String): List[String]
}
