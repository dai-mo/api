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

object MetaData {
  def apply(description: String, tags: List[String] = List(), related: List[String] = List()): MetaData =
    new MetaData(description, tags.asJava, related.asJava)
}
case class MetaData(@BeanProperty var description: String ,
                    @BeanProperty var tags: JavaList[String],
                    @BeanProperty var related: JavaList[String]) {
  def this() = this("", List().asJava, List().asJava)
}