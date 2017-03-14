package org.dcs.api.processor

import java.nio.ByteBuffer
import java.util
import java.util.{Map => JavaMap}

import com.google.common.net.MediaType
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericFixed, GenericRecord}
import org.dcs.commons.error.{ErrorConstants, ErrorResponse}
import org.dcs.commons.serde.AvroImplicits._
import org.dcs.commons.serde.AvroSchemaStore

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object RemoteProcessor {
  val ProcessorTypeKey = "_PROCESSOR_TYPE"

  val UnknownProcessorType = "unknown"
  val IngestionProcessorType = "ingestion"
  val WorkerProcessorType = "worker"
  val SinkProcessorType = "sink"
  val BatchProcessorType = "batch"
}

trait RemoteProcessor extends BaseProcessor
  with ProcessorDefinition {
  import RemoteProcessor._

  private val parser = new Parser

  def execute(record: Option[GenericRecord], properties: JavaMap[String, String]): List[Either[ErrorResponse, AnyRef]]

  def trigger(input: Array[Byte], properties: JavaMap[String, String]): Array[Array[Byte]] = {
    val coreProperties: CoreProperties = CoreProperties(properties.asScala.toMap)

    try {
      val (resolvedSchemaId, schema) = resolveSchema(coreProperties, className)
      val in = Option(input).map(input => if(input.isEmpty) null else input.deSerToGenericRecord(schema, schema))

      execute(in, properties).flatMap { out =>
        try {
          if (out.isLeft)
            Array(RelationshipType.FailureRelationship.getBytes,
              AvroSchemaStore.ErrorResponseSchema.getBytes,
              out.left.get.serToBytes(Some(AvroSchemaStore.errorResponseSchema())))
          else {
            Array(RelationshipType.SucessRelationship.getBytes,
              resolvedSchemaId.getOrElse("").getBytes,
              out.right.get.serToBytes(schema))
          }
        } catch {
          case NonFatal(t) => Array(RelationshipType.FailureRelationship.getBytes,
            AvroSchemaStore.ErrorResponseSchema.getBytes,
            ErrorConstants.DCS306.withErrorMessage(t.getMessage).avroRecord().serToBytes(Some(AvroSchemaStore.errorResponseSchema())))
        }
      }.toArray
    } catch {
      case NonFatal(t) => Array(RelationshipType.FailureRelationship.getBytes,
        AvroSchemaStore.ErrorResponseSchema.getBytes,
        ErrorConstants.DCS306.withErrorMessage(t.getMessage).avroRecord().serToBytes(Some(AvroSchemaStore.errorResponseSchema())))
    }
  }

  def processorType(): String

  def className: String

  def schemaId = className

  private def resolveSchema(coreProperties: CoreProperties, className: String): (Option[String], Option[Schema]) = processorType() match {
    // This method should ideally be implemented in the RemoteProcessor sub traits (Ingestion, Worker, ...)
    // implying that the method declared in the RemoteProcessor class should be public or protected
    // but since all public or protected the methods in Remote Processor are exposed over soap,
    // this method will also be unnecessarily exposed over soap. Hence the reason why this is a private method
    // in RemoteProcessor
    case IngestionProcessorType => {
      var schema = coreProperties.schema
      var sid: Option[String] = None
      if(schema.isEmpty) {
        schema = AvroSchemaStore.get(schemaId)
        if(schema.isEmpty) throw new IllegalStateException("Schema for Ingestion Processor " + className + " not available")
        sid = Option(schemaId)
      }
      (sid, schema)
    }
    case WorkerProcessorType => {
      var schema = coreProperties.schema
      var sid: Option[String] = None
      if(schema.isEmpty) {
        sid = coreProperties.schemaId
        schema = sid.flatMap(AvroSchemaStore.get)
        if(schema.isEmpty) throw new IllegalStateException("Schema for Worker Processor " + className + " not available")
      }
      (sid, schema)
    }
    case SinkProcessorType => {
      var schema = coreProperties.schema
      var sid: Option[String] = None
      if(schema.isEmpty) {
        sid = coreProperties.schemaId
        schema = sid.flatMap(AvroSchemaStore.get)
        if(schema.isEmpty) throw new IllegalStateException("Schema for Sink Processor " + className + " not available")
      }
      (sid, schema)
    }
    case _ => throw new IllegalStateException("Unknown processor type : " + processorType)
  }

  implicit class GenericRecordTypes(record: Option[GenericRecord]) {
    def getAsDouble(key: String, properties: JavaMap[String, String] = null): Option[Double] =
      if(properties == null)
        record.map(_.get(key)).map(_.asInstanceOf[Double])
      else
        mapsTo(record, key, properties).map(_.asInstanceOf[Double])

    def getAsBoolean(key: String, properties: JavaMap[String, String] = null): Option[Boolean] =
      if(properties == null)
        record.map(_.get(key)).map(_.asInstanceOf[Boolean])
      else
        mapsTo(record, key, properties).map(_.asInstanceOf[Boolean])

    def getAsInt(key: String, properties: JavaMap[String, String] = null): Option[Int] =
      if(properties == null)
        record.map(_.get(key)).map(_.asInstanceOf[Int])
      else
        mapsTo(record, key, properties).map(_.asInstanceOf[Int])

    def getAsLong(key: String, properties: JavaMap[String, String] = null): Option[Long] =
      if(properties == null)
        record.map(_.get(key)).map(_.asInstanceOf[Int])
      else
        mapsTo(record, key, properties).map(_.asInstanceOf[Int])
    def getAsFloat(key: String, properties: JavaMap[String, String] = null): Option[Float] =
      if(properties == null)
        record.map(_.get(key)).map(_.asInstanceOf[Int])
      else
        mapsTo(record, key, properties).map(_.asInstanceOf[Int])

    def getAsByteBuffer(key: String, properties: JavaMap[String, String] = null): Option[ByteBuffer] =
      if(properties == null)
        record.map(_.get(key)).map(_.asInstanceOf[ByteBuffer])
      else
        mapsTo(record, key, properties).map(_.asInstanceOf[ByteBuffer])

    def getAsCharSequence(key: String, properties: JavaMap[String, String] = null): Option[CharSequence] =
      if(properties == null)
        record.map(_.get(key)).map(_.asInstanceOf[CharSequence])
      else
        mapsTo(record, key, properties).map(_.asInstanceOf[CharSequence])

    def getAsGenericRecord(key: String, properties: JavaMap[String, String] = null): Option[GenericRecord] =
      if(properties == null)
        record.map(_.get(key)).map(_.asInstanceOf[GenericRecord])
      else
        mapsTo(record, key, properties).map(_.asInstanceOf[GenericRecord])

    def getAsList[T](key: String, properties: JavaMap[String, String] = null): Option[List[T]] =
      if(properties == null)
        record.map(_.get(key)).map(_.asInstanceOf[List[T]])
      else
        mapsTo(record, key, properties).map(_.asInstanceOf[List[T]])
    def getAsMap[K, V](key: String, properties: JavaMap[String, String] = null): Option[Map[K, V]] =
      if(properties == null)
        record.map(_.get(key)).map(_.asInstanceOf[Map[K, V]])
      else
        mapsTo(record, key, properties).map(_.asInstanceOf[Map[K, V]])

    def getAsGenericFixed(key: String, properties: JavaMap[String, String] = null): Option[GenericFixed] =
      if(properties == null)
        record.map(_.get(key)).map(_.asInstanceOf[GenericFixed])
      else
        mapsTo(record, key, properties).map(_.asInstanceOf[GenericFixed])

    private def mapsTo(record: Option[GenericRecord], key: String, properties: JavaMap[String, String]): Option[Object] = {
      def get(path: List[String], currentRecord: Option[GenericRecord]): Option[Object] = path match {
        case last :: Nil => currentRecord.map(_.get(last))
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

trait ProcessorDefinition extends HasProperties
  with HasRelationships
  with HasConfiguration
  with HasMetaData

trait Ingestion extends RemoteProcessor {
  override def processorType(): String = RemoteProcessor.IngestionProcessorType

  override def configuration: Configuration = Configuration(inputMimeType = MediaType.OCTET_STREAM.toString,
    outputMimeType = MediaType.OCTET_STREAM.toString,
    processorClassName =  className,
    inputRequirementType = InputRequirementType.InputForbidden)

}

trait Worker extends RemoteProcessor {
  override def processorType(): String = RemoteProcessor.WorkerProcessorType

  def configuration: Configuration = Configuration(inputMimeType = MediaType.OCTET_STREAM.toString,
    outputMimeType = MediaType.OCTET_STREAM.toString,
    processorClassName =  className,
    inputRequirementType = InputRequirementType.InputRequired)

  override def schemaId = className
}

trait Sink extends RemoteProcessor {
  override def processorType(): String = RemoteProcessor.SinkProcessorType

  override def configuration: Configuration = Configuration(inputMimeType = MediaType.OCTET_STREAM.toString,
    outputMimeType = MediaType.OCTET_STREAM.toString,
    processorClassName =  className,
    inputRequirementType = InputRequirementType.InputRequired)
}




