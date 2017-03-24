package org.dcs.api.processor

import java.util
import java.util.{List => JavaList, Map => JavaMap}

import org.apache.avro.generic.GenericRecord
import org.dcs.commons.serde.JsonSerializerImplicits._

import scala.collection.JavaConverters._
/**
  * Created by cmathew on 10.03.17.
  */
trait FieldsToMap extends RemoteProcessor {

  def fields: List[String]

  override def properties(): JavaList[RemoteProperty] = {
    var props = super.properties()

    if(fields.nonEmpty)
      props = new util.ArrayList(props)
      props.add(CoreProperties.fieldsToMapProperty(fields.map(f => (f, "$." + f)).toMap.toJson))
    props
  }

  def mappings(record: Option[GenericRecord], properties: Map[String, String]): Map[String, Object] =
    record.mappings(properties.asJava)

  implicit class GenericRecordTypes(record: Option[GenericRecord]) {

    def mappings(properties: JavaMap[String, String]): Map[String, Object] = {
      properties.asScala.
        find(p => p._1 == CoreProperties.FieldsToMapKey).
        map(p => p._2.toMapOf[String]).
        map(fmap => fmap.map(fmap => (fmap._1, record.getFromJsonPath(fmap._2.split("\\.").toList))).
          filter(fmap => fmap._2.isDefined).
          map(fmap => (fmap._1, fmap._2.get))).
        getOrElse(Map[String, Object]())
    }
  }
}
