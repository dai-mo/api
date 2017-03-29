package org.dcs.api.processor

import java.util
import java.util.{List => JavaList, Map => JavaMap}

import org.apache.avro.generic.GenericRecord

import scala.collection.JavaConverters._
import org.dcs.commons.serde.JsonSerializerImplicits._

import scala.beans.BeanProperty

/**
  * Created by cmathew on 24.03.17.
  */

case class Action(@BeanProperty var jsonPath: String,
                  @BeanProperty var cmd: String,
                  @BeanProperty var args: String) {
  def this() = this("", "", "")

  def fromJsonPath(record: Option[GenericRecord]): Option[GenericRecordObject] = {
    RemoteProcessor.fromJsonPath(jsonPath, record)
  }
}

trait FieldActions extends RemoteProcessor {

  def cmds: List[String]

  override def properties(): JavaList[RemoteProperty] = {
    val props = new util.ArrayList(super.properties())

    if(cmds.nonEmpty)
      props.add(CoreProperties.fieldActionsProperty(cmds.map(c => Action("", c, "")).toJson))
    props
  }

  def actions(properties: JavaMap[String, String]): List[Action] = {
    properties.asScala.
      find(p => p._1 == CoreProperties.FieldActionsKey).
      map(p => p._2.asList[Action].
        filter(a => a.jsonPath != null && a.jsonPath.nonEmpty)).
      getOrElse(Nil)
  }

}
