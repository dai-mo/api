package org.dcs.api.processor

import java.util.{List => JavaList}

import scala.beans.BeanProperty
import scala.collection.JavaConverters._

/**
  * Created by cmathew on 06/09/16.
  */
trait HasMetaData {

  def metadata():MetaData
}

case class MetaData(@BeanProperty var description: String = "",
                    @BeanProperty var tags: JavaList[String] = List().asJava,
                    @BeanProperty var related: JavaList[String] = List().asJava) {
  def this() = this("", List().asJava, List().asJava)
}