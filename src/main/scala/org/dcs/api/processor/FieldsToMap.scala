/*
 * Copyright (c) 2017-2018 brewlabs SAS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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

case class MappedValue(jsonPath: String, value: Object, fieldType: String)

trait FieldsToMap extends RemoteProcessor {

  def fields: Set[ProcessorSchemaField]



  override def properties(): JavaList[RemoteProperty] = {
    val props = new util.ArrayList(super.properties())

    if(fields.nonEmpty)
      props.add(CoreProperties.fieldsToMapProperty(fields))
    props
  }

  def mappings(record: Option[GenericRecord], properties: Map[String, String]): Map[String, List[MappedValue]] =
    record.mappings(properties.asJava)

  implicit class GenericRecordTypes(record: Option[GenericRecord]) {

    def mappings(properties: JavaMap[String, String]): Map[String, List[MappedValue]] = {

      properties.asScala.
        find(p => p._1 == CoreProperties.FieldsToMapKey)
        .map(p => p._2.asList[ProcessorSchemaField])
        .map(flist => flist.map(f => (f.name, (f.jsonPath, record.fromJsonPath(f.jsonPath), f.fieldType)))
          .map(f => (f._1, (f._2._1, f._2._2.flatMap(_.value), f._2._3)))
          .filter(fgr => fgr._2._2.isDefined)
          .map(fgr => (fgr._1, MappedValue(fgr._2._1, fgr._2._2.get, fgr._2._3)))
          .groupBy(k => k._1)
          .mapValues(t => t.map(_._2)))
        .getOrElse(Map())
    }
  }


  implicit class MappingUtils(mappingValues: Option[List[MappedValue]]) {

    def mappedValues[T](): List[(String,T)] = {
      mappingValues.map(mvl => mvl.map(mv => (mv.jsonPath, cast[T](mv.value, mv.fieldType))))
        .getOrElse(Nil)
    }

    def values[T](): List[T] = {
      mappingValues.map(mvl => mvl.map(mv => cast[T](mv.value, mv.fieldType)))
        .getOrElse(Nil)
    }

    def asMap[T](): Map[String, T] = {
      mappingValues.map(mvl => mvl.map(mv => mv.jsonPath -> cast[T](mv.value, mv.fieldType)).toMap)
        .getOrElse(Map())
    }
  }
}
