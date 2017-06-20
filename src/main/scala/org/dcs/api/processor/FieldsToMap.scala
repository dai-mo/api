package org.dcs.api.processor

import java.util
import java.util.{List => JavaList, Map => JavaMap}

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.dcs.commons.SchemaField
import org.dcs.commons.serde.JsonSerializerImplicits._

import scala.collection.JavaConverters._
/**
  * Created by cmathew on 10.03.17.
  */
object FieldsToMap {

  def schemaCheck(schema: Schema, fieldsToMap: String): Boolean = {
    fieldsToMap.toMapOf[String].foreach(ftm =>
      if(!SchemaField.validatePath(schema, ftm._2))
        throw new IllegalStateException("Required field " + ftm._2 + "does not exist in schema"))
    true
  }
}
trait FieldsToMap extends RemoteProcessor {

  def fields: List[String]

  override def properties(): JavaList[RemoteProperty] = {
    val props = new util.ArrayList(super.properties())

    if(fields.nonEmpty)
      props.add(CoreProperties.fieldsToMapProperty(fields.map(f => (f, "")).toMap.toJson))
    props
  }

  def mappings(record: Option[GenericRecord], properties: Map[String, String]): Map[String, Object] =
    record.mappings(properties.asJava)

  implicit class GenericRecordTypes(record: Option[GenericRecord]) {

    def mappings(properties: JavaMap[String, String]): Map[String, Object] = {
      properties.asScala.
        find(p => p._1 == CoreProperties.FieldsToMapKey).
        map(p => p._2.toMapOf[String]).
        map(fmap => fmap.map(fmap => (fmap._1, record.fromJsonPath(fmap._2))).
          map(fmap => (fmap._1, fmap._2.flatMap(_.value))).
          filter(fmap => fmap._2.isDefined).
          map(fmap => (fmap._1, fmap._2.get))).
        getOrElse(Map[String, Object]())
    }
  }
}
