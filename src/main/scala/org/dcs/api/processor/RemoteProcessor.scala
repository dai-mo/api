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

  def resolveReadSchema(coreProperties: CoreProperties): (Option[String], Option[Schema]) = {
    var schema = coreProperties.readSchema
    var sid: Option[String] = None
    if(schema.isEmpty) {
      sid = coreProperties.readSchemaId
      if(sid.isDefined) schema = AvroSchemaStore.get(sid.get)
    }
    (sid, schema)
  }

  def resolveWriteSchema(coreProperties: CoreProperties): (Option[String], Option[Schema]) = {
    var schema = coreProperties.writeSchema
    var sid: Option[String] = None
    if(schema.isEmpty) {
      sid = coreProperties.writeSchemaId
      if(sid.isDefined) schema = AvroSchemaStore.get(sid.get)
    }
    (sid, schema)
  }
}

trait RemoteProcessor extends BaseProcessor
  with ProcessorDefinition {
  import RemoteProcessor._

  private val parser = new Parser

  def execute(record: Option[GenericRecord], properties: JavaMap[String, String]): List[Either[ErrorResponse, AnyRef]]

  def trigger(input: Array[Byte], properties: JavaMap[String, String]): Array[Array[Byte]] = {
    val coreProperties: CoreProperties = CoreProperties(properties.asScala.toMap)

    try {
      val (readSchemaId, readSchema) = resolveReadSchema(coreProperties)
      if(readSchemaId.isEmpty && readSchema.isEmpty)
        throw new IllegalStateException("Read Schema for  " + className + " not available")

      val (writeSchemaId, writeSchema) = resolveReadSchema(coreProperties)
      if(readSchemaId.isEmpty && writeSchema.isEmpty)
        throw new IllegalStateException("Write Schema for  " + className + " not available")

      val in = Option(input).map(input => if(input.isEmpty) null else input.deSerToGenericRecord(readSchema, readSchema))

      execute(in, properties).flatMap { out =>
        try {
          if (out.isLeft)
            Array(RelationshipType.FailureRelationship.getBytes,
              AvroSchemaStore.ErrorResponseSchema.getBytes,
              out.left.get.serToBytes(Some(AvroSchemaStore.errorResponseSchema())))
          else {
            Array(RelationshipType.SucessRelationship.getBytes,
              writeSchemaId.getOrElse("").getBytes,
              out.right.get.serToBytes(writeSchema))
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

  def schemaId: String = ""

  // The methods 'resolveReadSchema' and 'resolveWriteSchema' should ideally be implemented
  // in the RemoteProcessor sub traits (Ingestion, Worker, ...)
  // implying that the method declared in the RemoteProcessor class should be public or protected
  // but since all public or protected the methods in Remote Processor are exposed over soap,
  // this method will also be unnecessarily exposed over soap. Hence the reason why this is a private method
  // in RemoteProcessor
  private def resolveReadSchema(coreProperties: CoreProperties): (Option[String], Option[Schema]) = processorType() match {
    case IngestionProcessorType => Ingestion.resolveReadSchema(coreProperties, Option(schemaId))
    case WorkerProcessorType | SinkProcessorType => RemoteProcessor.resolveReadSchema(coreProperties)
    case _ => throw new IllegalStateException("Unknown processor type : " + processorType)
  }

  private def resolveWriteSchema(coreProperties: CoreProperties, className: String): (Option[String], Option[Schema]) = processorType() match {
    case IngestionProcessorType => Ingestion.resolveWriteSchema(coreProperties, Option(schemaId))
    case WorkerProcessorType | SinkProcessorType => RemoteProcessor.resolveWriteSchema(coreProperties)
    case _ => throw new IllegalStateException("Unknown processor type : " + processorType)
  }


}

trait ProcessorDefinition extends HasProperties
  with HasRelationships
  with HasConfiguration
  with HasMetaData

object Ingestion {

  def resolveReadSchema(coreProperties: CoreProperties, schemaId: Option[String]): (Option[String], Option[Schema]) = {
    (schemaId, schemaId.flatMap(id => AvroSchemaStore.get(id)))
  }

  def resolveWriteSchema(coreProperties: CoreProperties, schemaId: Option[String]): (Option[String], Option[Schema]) = {
    val idOrSchema = RemoteProcessor.resolveWriteSchema(coreProperties)
    var sid: Option[String] = idOrSchema._1
    var schema:Option[Schema] = idOrSchema._2

    if(idOrSchema._1.isEmpty && idOrSchema._2.isEmpty) {
      sid = schemaId
      schema = schemaId.flatMap(id => AvroSchemaStore.get(id))

    }
    (sid, schema)
  }
}

trait Ingestion extends RemoteProcessor {
  override def processorType(): String = RemoteProcessor.IngestionProcessorType

  override def configuration: Configuration = Configuration(inputMimeType = MediaType.OCTET_STREAM.toString,
    outputMimeType = MediaType.OCTET_STREAM.toString,
    processorClassName =  className,
    inputRequirementType = InputRequirementType.InputForbidden)

  override def schemaId: String = className

}

trait Worker extends RemoteProcessor {
  override def processorType(): String = RemoteProcessor.WorkerProcessorType

  def configuration: Configuration = Configuration(inputMimeType = MediaType.OCTET_STREAM.toString,
    outputMimeType = MediaType.OCTET_STREAM.toString,
    processorClassName =  className,
    inputRequirementType = InputRequirementType.InputRequired)

}

trait Sink extends RemoteProcessor {
  override def processorType(): String = RemoteProcessor.SinkProcessorType

  override def configuration: Configuration = Configuration(inputMimeType = MediaType.OCTET_STREAM.toString,
    outputMimeType = MediaType.OCTET_STREAM.toString,
    processorClassName =  className,
    inputRequirementType = InputRequirementType.InputRequired)
}




