package org.dcs.api.service

import scala.beans.BeanProperty

/**
  * Created by cmathew on 05/06/16.
  */
case class Module(@BeanProperty var productId: String,
                  @BeanProperty var description: String,
                  @BeanProperty var displayName: String) {
  def this() = this("", "", "")
}

trait ModulesApiService {
  def list(mType: String): List[Module]
}
