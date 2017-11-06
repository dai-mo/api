package org.dcs.api.processor

import java.util

/**
  * @author cmathew
  */
trait GlobalControl {

  def preStart(properties: util.Map[String, String]): Boolean = true
  def preStop(properties: util.Map[String, String]): Boolean = true
  def postRemove(properties: util.Map[String, String]): Boolean = true
}
