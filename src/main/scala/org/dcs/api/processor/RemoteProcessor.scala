package org.dcs.api.processor

import java.lang.NullPointerException
import java.nio.ByteBuffer
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

case class GenericRecordObject(parent: GenericRecord, key: String) {
  def value: Option[Object] = Option(parent.get(key))
}

object RemoteProcessor {
  val ProcessorTypeKey = "_PROCESSOR_TYPE"

  val UnknownProcessorType = "unknown"
  val IngestionProcessorType = "ingestion"
  val WorkerProcessorType = "worker"
  val SinkProcessorType = "sink"
  val BatchProcessorType = "batch"


  def resolveReadSchema(coreProperties: CoreProperties): Option[Schema] = {
    var schema = coreProperties.readSchema

    if (schema.isEmpty) {
      schema = coreProperties.readSchemaId.flatMap(AvroSchemaStore.get)
    }

    schema
  }


  def resolveWriteSchema(coreProperties: CoreProperties, schemaId: Option[String]): Option[Schema] = {
    var schema = coreProperties.writeSchema

    if(schema.isEmpty) {
      schema =  coreProperties.writeSchemaId.flatMap(AvroSchemaStore.get)
    }

    if(schema.isEmpty) {
      schema = schemaId.flatMap(AvroSchemaStore.get)
    }

    if(schema.isEmpty)
      resolveReadSchema(coreProperties)
    else
      schema
  }
}

trait RemoteProcessor extends BaseProcessor
  with ProcessorDefinition {
  import RemoteProcessor._

  def execute(record: Option[GenericRecord], properties: JavaMap[String, String]): List[Either[ErrorResponse, AnyRef]]

  def trigger(input: Array[Byte], properties: JavaMap[String, String]): Array[Array[Byte]] = {
    val coreProperties: CoreProperties = CoreProperties(properties.asScala.toMap)


    try {

      var readSchema = resolveReadSchema(coreProperties)
      if(!input.isEmpty && readSchema.isEmpty)
        throw new IllegalStateException("Read Schema for  " + className + " not available")


      val writeSchema = resolveWriteSchema(coreProperties)
      if(writeSchema.isEmpty)
        throw new IllegalStateException("Write Schema for  " + className + " not available")

      val in = Option(input).map(input => if(input.isEmpty) null else input.deSerToGenericRecord(readSchema, readSchema))

      execute(in, properties).flatMap { out =>
        try {
          out match {
            case Left(error) =>  Array(RelationshipType.FailureRelationship.getBytes,
              error.serToBytes(Some(AvroSchemaStore.errorResponseSchema())))
            case Right(record) => Array(RelationshipType.SucessRelationship.getBytes,
              // FIXME: This really needs to be optimised (to avoid double ser / deser)
              //        This is due to the fact that GenericRecord is essentially immutable
              //        We need to extend GenericRecord to allow for removing fields
              if(input.isEmpty || (schemaId != null && schemaId.nonEmpty))
                record.serToBytes(writeSchema)
              else
                record
                  .serToBytes(readSchema)
                  .deSerToGenericRecord(readSchema, writeSchema)
                  .serToBytes(writeSchema))
          }
        } catch {
          case NonFatal(t) => Array(RelationshipType.FailureRelationship.getBytes,
            ErrorConstants.DCS306.withErrorMessage(Option(t.getMessage).getOrElse(t.getClass.getName)).
              avroRecord().
              serToBytes(Some(AvroSchemaStore.errorResponseSchema())))
        }
      }.toArray
    } catch {
      case NonFatal(t) => Array(RelationshipType.FailureRelationship.getBytes,
        ErrorConstants.DCS306.withErrorMessage(Option(t.getMessage).getOrElse(t.getClass.getName)).
          avroRecord().
          serToBytes(Some(AvroSchemaStore.errorResponseSchema())))
    }
  }

  def processorType(): String

  def className: String = this.getClass.getName

  def schemaId: String = null

  // The methods 'resolveReadSchema' and 'resolveWriteSchema' should ideally be implemented
  // in the RemoteProcessor sub traits (Ingestion, Worker, ...)
  // implying that the method declared in the RemoteProcessor class should be public or protected
  // but since all public or protected the methods in Remote Processor are exposed over soap,
  // this method will also be unnecessarily exposed over soap. Hence the reason why this is a private method
  // in RemoteProcessor
  private def resolveReadSchema(coreProperties: CoreProperties): Option[Schema] = processorType() match {
    case IngestionProcessorType => None
    case WorkerProcessorType | SinkProcessorType => RemoteProcessor.resolveReadSchema(coreProperties)
    case _ => throw new IllegalStateException("Unknown processor type : " + processorType)
  }

  private def resolveWriteSchema(coreProperties: CoreProperties): Option[Schema] = processorType() match {
    case IngestionProcessorType | WorkerProcessorType | SinkProcessorType => RemoteProcessor.resolveWriteSchema(coreProperties, Option(schemaId))
    case _ => throw new IllegalStateException("Unknown processor type : " + processorType)
  }


  implicit class GenericRecordFields(record: Option[GenericRecord]) {
    def asDouble(key: String): Option[Double] =
      record.flatMap(r => Option(r.get(key))).map(_.asInstanceOf[Double])

    def asBoolean(key: String): Option[Boolean] =
      record.flatMap(r => Option(r.get(key))).map(_.asInstanceOf[Boolean])

    def asInt(key: String): Option[Int] =
      record.flatMap(r => Option(r.get(key))).map(_.asInstanceOf[Int])

    def asLong(key: String): Option[Long] =
      record.flatMap(r => Option(r.get(key))).map(_.asInstanceOf[Long])

    def asFloat(key: String): Option[Float] =
      record.flatMap(r => Option(r.get(key))).map(_.asInstanceOf[Float])

    def asString(key: String): Option[String] =
      record.flatMap(r => Option(r.get(key))).map(_.asInstanceOf[String])

    def asByteBuffer(key: String): Option[ByteBuffer] =
      record.flatMap(r => Option(r.get(key))).map(_.asInstanceOf[ByteBuffer])

    def asCharSequence(key: String): Option[CharSequence] =
      record.flatMap(r => Option(r.get(key))).map(_.asInstanceOf[CharSequence])

    def asGenericRecord(key: String): Option[GenericRecord] =
      record.flatMap(r => Option(r.get(key))).map(_.asInstanceOf[GenericRecord])

    def asList[T](key: String): Option[List[T]] =
      record.flatMap(r => Option(r.get(key))).map(_.asInstanceOf[List[T]])

    def asMap[K, V](key: String): Option[Map[K, V]] =
      record.flatMap(r => Option(r.get(key))).map(_.asInstanceOf[Map[K, V]])

    def asGenericFixed(key: String): Option[GenericFixed] =
      record.flatMap(r => Option(r.get(key))).map(_.asInstanceOf[GenericFixed])

    def fromJsonPath(path: List[String]): Option[GenericRecordObject] = {
      fromJsonPath(path, record)
    }

    private def fromJsonPath(path: List[String], currentRecord: Option[GenericRecord]): Option[GenericRecordObject] = path match {
      case Nil => None
      case last :: Nil => currentRecord.map(r => GenericRecordObject(r, last))
      case "$" :: tail => currentRecord.flatMap(r => fromJsonPath(tail, Option(r)))
      case head :: tail => currentRecord.flatMap(r => fromJsonPath(tail, Option(r.get(head)).map(_.asInstanceOf[GenericRecord])))
    }
  }

  implicit class GenericRecordCasts(value: Option[Object]) {
    def asDouble: Option[Double] = value.map(_.asInstanceOf[Double])

    def asBoolean: Option[Boolean] = value.map(_.asInstanceOf[Boolean])

    def asInt: Option[Int] = value.map(_.asInstanceOf[Int])

    def asLong: Option[Long] = value.map(_.asInstanceOf[Long])

    def asFloat: Option[Float] = value.map(_.asInstanceOf[Float])

    def asString: Option[String] = value.map(_.asInstanceOf[String])

    def asByteBuffer: Option[ByteBuffer] = value.map(_.asInstanceOf[ByteBuffer])

    def asCharSequence: Option[CharSequence] = value.map(_.asInstanceOf[CharSequence])

    def asGenericRecord: Option[GenericRecord] = value.map(_.asInstanceOf[GenericRecord])

    def asList[T]: Option[List[T]] = value.map(_.asInstanceOf[List[T]])

    def asMap[K, V]: Option[Map[K, V]] = value.map(_.asInstanceOf[Map[K, V]])

    def asGenericFixed: Option[GenericFixed] = value.map(_.asInstanceOf[GenericFixed])
  }

  implicit class GenericRecordObjectAccess(gro: Option[GenericRecordObject]) {
    def value: Option[Object] = gro.flatMap(r => r.value)
  }
}

trait ProcessorDefinition extends HasProperties
  with HasRelationships
  with HasConfiguration
  with HasMetaData



trait Ingestion extends RemoteProcessor {
  if(schemaId != null && !schemaId.isEmpty) AvroSchemaStore.add(schemaId)

  override def processorType(): String = RemoteProcessor.IngestionProcessorType

  override def configuration: Configuration = Configuration(inputMimeType = MediaType.OCTET_STREAM.toString,
    outputMimeType = MediaType.OCTET_STREAM.toString,
    processorClassName =  className,
    inputRequirementType = InputRequirementType.InputForbidden)

  override def schemaId: String = className

}

trait Worker extends RemoteProcessor {
  if(schemaId != null && !schemaId.isEmpty) AvroSchemaStore.add(schemaId)

  override def processorType(): String = RemoteProcessor.WorkerProcessorType

  def configuration: Configuration = Configuration(inputMimeType = MediaType.OCTET_STREAM.toString,
    outputMimeType = MediaType.OCTET_STREAM.toString,
    processorClassName =  className,
    inputRequirementType = InputRequirementType.InputRequired)

}

trait Sink extends RemoteProcessor {
  if(schemaId != null && !schemaId.isEmpty) AvroSchemaStore.add(schemaId)

  override def processorType(): String = RemoteProcessor.SinkProcessorType

  override def configuration: Configuration = Configuration(inputMimeType = MediaType.OCTET_STREAM.toString,
    outputMimeType = MediaType.OCTET_STREAM.toString,
    processorClassName =  className,
    inputRequirementType = InputRequirementType.InputRequired)
}




