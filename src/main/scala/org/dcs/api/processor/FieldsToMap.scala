package org.dcs.api.processor

import java.nio.ByteBuffer
import java.util
import java.util.{List => JavaList, Map => JavaMap}

import org.apache.avro.generic.{GenericFixed, GenericRecord}
import org.dcs.commons.error.ErrorResponse

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


  override def trigger(input: Array[Byte], properties: JavaMap[String, String]): Array[Array[Byte]] = {
    implicit val props = properties
    super.trigger(input, properties)
  }

  implicit class GenericRecordTypes(record: Option[GenericRecord]) {
    def getAsDouble(key: String)(implicit properties: JavaMap[String, String] = null): Option[Double] =
      if(properties == null)
        record.getAsDouble(key)
      else
        mapsTo(record, key, properties).map(_.asInstanceOf[Double])

    def getAsBoolean(key: String)(implicit properties: JavaMap[String, String] = null): Option[Boolean] =
      if(properties == null)
        record.getAsBoolean(key)
      else
        mapsTo(record, key, properties).map(_.asInstanceOf[Boolean])

    def getAsInt(key: String)(implicit properties: JavaMap[String, String] = null): Option[Int] =
      if(properties == null)
        record.getAsInt(key)
      else
        mapsTo(record, key, properties).map(_.asInstanceOf[Int])

    def getAsLong(key: String)(implicit properties: JavaMap[String, String] = null): Option[Long] =
      if(properties == null)
        record.getAsLong(key)
      else
        mapsTo(record, key, properties).map(_.asInstanceOf[Long])

    def getAsFloat(key: String)(implicit properties: JavaMap[String, String] = null): Option[Float] =
      if(properties == null)
        record.getAsFloat(key)
      else
        mapsTo(record, key, properties).map(_.asInstanceOf[Float])

    def getAsString(key: String)(implicit properties: JavaMap[String, String] = null): Option[String] =
      if(properties == null)
        record.getAsString(key)
      else
        mapsTo(record, key, properties).map(_.asInstanceOf[String])

    def getAsByteBuffer(key: String)(implicit properties: JavaMap[String, String] = null): Option[ByteBuffer] =
      if(properties == null)
        record.getAsByteBuffer(key)
      else
        mapsTo(record, key, properties).map(_.asInstanceOf[ByteBuffer])

    def getAsCharSequence(key: String)(implicit properties: JavaMap[String, String] = null): Option[CharSequence] =
      if(properties == null)
        record.getAsCharSequence(key)
      else
        mapsTo(record, key, properties).map(_.asInstanceOf[CharSequence])

    def getAsGenericRecord(key: String)(implicit properties: JavaMap[String, String] = null): Option[GenericRecord] =
      if(properties == null)
        record.getAsGenericRecord(key)
      else
        mapsTo(record, key, properties).map(_.asInstanceOf[GenericRecord])

    def getAsList[T](key: String)(implicit properties: JavaMap[String, String] = null): Option[List[T]] =
      if(properties == null)
        record.getAsList[T](key)
      else
        mapsTo(record, key, properties).map(_.asInstanceOf[List[T]])

    def getAsMap[K, V](key: String)(implicit properties: JavaMap[String, String] = null): Option[Map[K, V]] =
      if(properties == null)
        record.getAsMap[K, V](key)
      else
        mapsTo(record, key, properties).map(_.asInstanceOf[Map[K, V]])

    def getAsGenericFixed(key: String)(implicit properties: JavaMap[String, String] = null): Option[GenericFixed] =
      if(properties == null)
        record.getAsGenericFixed(key)
      else
        mapsTo(record, key, properties).map(_.asInstanceOf[GenericFixed])

    private def mapsTo(record: Option[GenericRecord], key: String, properties: JavaMap[String, String]): Option[Object] = {
      def get(path: List[String], currentRecord: Option[GenericRecord]): Option[Object] = path match {
        case Nil => currentRecord
        case last :: Nil => currentRecord.flatMap(r => Option(r.get(last)))
        case "$" :: tail => get(path.tail, record)
        case head :: tail => get(path.tail, getAsGenericRecord(head))
      }
      val value = properties.asScala.
        find(p => p._1.startsWith(CoreProperties.FieldToMapKey) && p._1.endsWith(key)).
        map(_._2.split(".")).
        flatMap(path => get(path.toList, record))

      if(value.isDefined)
        value
      else
        record.map(_.get(key))
    }
  }
}
