package org.dcs.api.service

import scala.beans.BeanProperty

/**
  * Created by cmathew on 05/06/16.
  */

case class DataSource(@BeanProperty var uuid:String,
                      @BeanProperty var name: String,
                      @BeanProperty var uri: String) {
  def this() = this("", "", "")
}

trait DataApiService {
  def list(filter: DataSource): List[DataSource]
  def create(definition: DataSource): DataSource
}
