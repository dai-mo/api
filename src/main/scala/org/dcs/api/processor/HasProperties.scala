package org.dcs.api.processor

import java.util.{List => JavaList, Map => JavaMap, Set => JavaSet}

import scala.beans.BeanProperty
import scala.collection.JavaConverters._
import CoreProperties._

/**
  * Created by cmathew on 29/08/16.
  */
trait HasProperties {

  def properties(): JavaList[RemoteProperty] = (
    readSchemaIdProperty() :: readSchemaProperty() :: writeSchemaProperty() :: writeSchemaIdProperty(schemaId) ::
      _properties()).asJava

  protected def _properties(): List[RemoteProperty] =  Nil

  def schemaId: String

  def propertyValue(propertySettings: RemoteProperty, values: JavaMap[String, String]): String = {
    if(values == null)
      propertySettings.defaultValue
    else
      values.asScala.toMap.getOrElse(propertySettings.name, propertySettings.defaultValue)
  }


}

case class RemoteProperty(@BeanProperty var displayName: String,
                          @BeanProperty var name: String,
                          @BeanProperty var description: String,
                          @BeanProperty var defaultValue: String = null,
                          @BeanProperty var possibleValues: JavaSet[PossibleValue] = Set[PossibleValue]().asJava,
                          @BeanProperty var required: Boolean = false,
                          @BeanProperty var sensitive: Boolean = false,
                          @BeanProperty var dynamic: Boolean = false,
                          @BeanProperty var validators: JavaList[String]= List().asJava,
                          @BeanProperty var `type`: String = PropertyType.String,
                          @BeanProperty var level: Int = PropertyLevel.Open) {
  def this() = this("", "", "", "", Set[PossibleValue]().asJava, false, false, false, List().asJava, PropertyType.String, PropertyLevel.Open)
}

case class PossibleValue(@BeanProperty var value: String,
                         @BeanProperty var displayName: String,
                         @BeanProperty var description: String) {
  def this() = this("", "", "")
}

object PropertyType {
  val String = "STRING"
  val Number = "NUMBER"
  val Boolean = "BOOLEAN"
  val List = "LIST"
}

object PropertyLevel {
  val Open = 0
  val Expert = 10
  val Internal = 100
}



