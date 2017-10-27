package org.dcs.api.processor

import java.util

/**
  * @author cmathew
  */
trait GlobalControl {

  def start(properties: util.Map[String, String]): Boolean = true
  def stop(properties: util.Map[String, String]): Boolean = true
  def remove(properties: util.Map[String, String]): Boolean = true
}
