package org.dcs.api.service

import scala.beans.BeanProperty


case class TestResponse(@BeanProperty var response: String,
                        @BeanProperty var token: String) {
  def this() = this("", "")
}

trait TestApiService {
  def hello(name: String): TestResponse
}
