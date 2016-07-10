package org.dcs.api.service

import scala.beans.BeanProperty

/**
  * Created by cmathew on 29/06/16.
  */

case class User(@BeanProperty var id: String,
                @BeanProperty var name: String) {
  def this() = this("", "")
}

trait UserApiService {
  def users(): List[User]
  def user(userId: String): User
}
