package org.dcs.api.processor

import java.nio.ByteBuffer
import java.util
import java.util.{Map => JavaMap, List => JavaList}

import org.apache.avro.generic.{GenericFixed, GenericRecord}

import scala.collection.JavaConverters._

/**
  * Created by cmathew on 10.03.17.
  */
trait FieldsToMap extends RemoteProcessor {

  def fields: List[String]

  def fieldName(field: String): String = CoreProperties.FieldToMapKey + ":" + field

  override def properties(): JavaList[RemoteProperty] = {
    val props = super.properties()
    fields.foreach(f => {
      val fName = fieldName(f)
      props.add(RemoteProperty(displayName = fName,
        name = fName,
        description = "Field to map : " + f,
        required = true,
        defaultValue = "$." + f))
    })
    props
  }

  implicit class GenericRecordTypes(record: Option[GenericRecord]) {
    def getAsDouble(key: String, properties: util.List[RemoteProperty]): Option[Double] = record.map(_.get(key)).map(_.asInstanceOf[Double])
    def getAsBoolean(key: String): Option[Boolean] = record.map(_.get(key)).map(_.asInstanceOf[Boolean])
    def getAsInt(key: String): Option[Int] = record.map(_.get(key).asInstanceOf[Int])
    def getAsLong(key: String): Option[Long] = record.map(_.get(key).asInstanceOf[Long])
    def getAsFloat(key: String): Option[Float] = record.map(_.get(key).asInstanceOf[Float])
    def getAsByteBuffer(key: String): Option[ByteBuffer] = record.map(_.get(key).asInstanceOf[ByteBuffer])
    def getAsCharSequence(key: String): Option[CharSequence] = record.map(_.get(key).asInstanceOf[CharSequence])
    def getAsGenericRecord(key: String): Option[GenericRecord] = record.map(_.get(key).asInstanceOf[GenericRecord])
    def getAsList[T](key: String): Option[List[T]] = record.map(_.get(key).asInstanceOf[util.Collection[T]].asScala.toList)
    def getAsMap[K, V](key: String): Option[Map[K, V]] = record.map(_.get(key).asInstanceOf[util.Map[K,V]].asScala.toMap)
    def getAsGenericFixed(key: String): Option[GenericFixed] = record.map(_.get(key).asInstanceOf[GenericFixed])


  }


}
