package org.dcs.api.processor

import java.util.{List => JavaList, Map => JavaMap}

import scala.beans.BeanProperty
import scala.collection.JavaConverters._

/**
  * Created by cmathew on 29/08/16.
  */
trait HasProperties {

  def properties(): JavaList[PropertySettings]

  def propertyValue(propertySettings: PropertySettings, values: JavaMap[String, String]): String = {
    if(values == null)
      return propertySettings.defaultValue
    else
      values.asScala.toMap.getOrElse(propertySettings.name, propertySettings.defaultValue)
  }

}

case class PropertySettings(@BeanProperty var displayName: String,
                            @BeanProperty var name: String,
                            @BeanProperty var description: String,
                            @BeanProperty var defaultValue: String) {
  def this() = this("", "", "", "")
}


