package org.dcs.api.service

import scala.beans.BeanProperty


case class TestResponse(@BeanProperty var response: String) {
  def this() = this("")
}

trait TestApiService {

  def hello(name: String): TestResponse

  def error(eType: String): TestResponse
}
