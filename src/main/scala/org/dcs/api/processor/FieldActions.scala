package org.dcs.api.processor

import java.util
import java.util.{List => JavaList, Map => JavaMap}

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.dcs.commons.SchemaField

import scala.collection.JavaConverters._
import org.dcs.commons.serde.JsonSerializerImplicits._

import scala.beans.BeanProperty

/**
  * Created by cmathew on 24.03.17.
  */

case class Action(@BeanProperty name: String,
                  @BeanProperty fieldType: String,
                  @BeanProperty jsonPath: String = "",
                  @BeanProperty args: String = "") {
  def this() = this("", "", PropertyType.String, "")

  def fromJsonPath(record: Option[GenericRecord]): Option[GenericRecordObject] = {
    RemoteProcessor.fromJsonPath(jsonPath, record)
  }
}

object FieldActions {

  def schemaCheck(schema: Schema, fieldActions: String): Boolean = {
    fieldActions.asList[Action].foreach(fa =>
      if(fa.jsonPath.nonEmpty && !SchemaField.validatePath(schema, fa.jsonPath))
        throw new IllegalStateException("Required field " + fa.jsonPath + " does not exist in schema"))
    true
  }
}

trait FieldActions extends RemoteProcessor {

  def cmds: Set[Action]

  override def properties(): JavaList[RemoteProperty] = {
    val props = new util.ArrayList(super.properties())

    if(cmds.nonEmpty)
      props.add(CoreProperties.fieldActionsProperty(cmds))
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
