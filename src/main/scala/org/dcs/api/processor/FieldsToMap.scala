package org.dcs.api.processor


import java.util
import java.util.{List => JavaList, Map => JavaMap}

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.dcs.commons.serde.JsonSerializerImplicits._

import scala.beans.BeanProperty
import scala.collection.JavaConverters._
/**
  * Created by cmathew on 10.03.17.
  */

case class ProcessorSchemaField(@BeanProperty name: String,
                                @BeanProperty fieldType: String,
                                @BeanProperty jsonPath: String = "") {
  def this() = this("", PropertyType.String, "")
}

trait FieldsToMap extends RemoteProcessor {

  def fields: Set[ProcessorSchemaField]

  override def properties(): JavaList[RemoteProperty] = {
    val props = new util.ArrayList(super.properties())

    if(fields.nonEmpty)
      props.add(CoreProperties.fieldsToMapProperty(fields))
    props
  }

  def mappings(record: Option[GenericRecord], properties: Map[String, String]): Map[String, Object] =
    record.mappings(properties.asJava)

  implicit class GenericRecordTypes(record: Option[GenericRecord]) {

    def mappings(properties: JavaMap[String, String]): Map[String, Object] = {

      properties.asScala.
        find(p => p._1 == CoreProperties.FieldsToMapKey)
        .map(p => p._2.asList[ProcessorSchemaField])
        .map(flist => flist.map(f => (f.name, record.fromJsonPath(f.jsonPath)))
          .map(f => (f._1, f._2.flatMap(_.value)))
          .filter(fgr => fgr._2.isDefined)
          .map(fgr => (fgr._1, fgr._2.get))
          .toMap)
        .getOrElse(Map[String, Object]())

    }
  }
}
