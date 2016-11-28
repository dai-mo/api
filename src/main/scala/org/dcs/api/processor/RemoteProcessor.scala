package org.dcs.api.processor

import java.util.{List => JavaList, Map => JavaMap}

import org.apache.avro.generic.GenericRecord
import org.dcs.commons.error.{ErrorConstants, ErrorResponse}
import org.dcs.commons.serde.AvroImplicits._
import org.dcs.commons.serde.AvroSchemaStore

import scala.util.control.NonFatal

object RemoteProcessor {
  val SchemaIdKey = "SCHEMA_ID"

  val ProcessorTypeKey = "processor-type"

  val DataIngestionProcessorType = "data-ingestion"
  val WorkerProcessorType = "worker"
  val SinkProcessorType = "sink"
  val BatchProcessorType = "batch"
}

trait RemoteProcessor extends BaseProcessor
  with ProcessorDefinition {
  import RemoteProcessor._

  def execute(record: Option[GenericRecord], properties: JavaMap[String, String]): List[Either[ErrorResponse, AnyRef]]

  def trigger(input: Array[Byte], properties: JavaMap[String, String]): Array[Array[Byte]] = {
    val resolvedSchemaId = {
      val sid = properties.get(SchemaIdKey)
      if(sid == null)
        schemaId
      else
        sid
    }

    try {
      val schema = Option(resolvedSchemaId).flatMap(AvroSchemaStore.get)
      val in = Option(input).map(input => if(input.isEmpty) null else input.deSerToGenericRecord(schema, schema))

      execute(in, properties).flatMap { out =>
        try {
          if (out.isLeft)
            Array(RelationshipType.FailureRelationship.getBytes,
              AvroSchemaStore.ErrorResponseSchema.getBytes,
              out.left.get.serToBytes(Some(AvroSchemaStore.errorResponseSchema())))
          else {
            Array(RelationshipType.SucessRelationship.getBytes,
              resolvedSchemaId.getBytes,
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

  def schemaId: String
}

trait ProcessorDefinition extends HasProperties
  with HasRelationships
  with HasConfiguration
  with HasMetaData


