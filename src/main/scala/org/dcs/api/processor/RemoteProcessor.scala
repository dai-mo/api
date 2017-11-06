package org.dcs.api.processor

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util
import java.util.{List => JavaList, Map => JavaMap}

import com.google.common.net.MediaType
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericFixed, GenericRecord}
import org.dcs.api.processor.RelationshipType.Success
import org.dcs.api.service.ProcessorDetails
import org.dcs.commons.error.{ErrorConstants, ErrorResponse}
import org.dcs.commons.serde.AvroImplicits._
import org.dcs.commons.serde.{AvroSchemaStore, DataGenerator}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

case class GenericRecordObject(parent: GenericRecord, key: String) {
  def value: Option[Object] = Option(parent.get(key))
}

object RemoteProcessor {

  val UnknownProcessorType = "unknown"
  val IngestionProcessorType = "ingestion"
  val WorkerProcessorType = "worker"
  val SinkProcessorType = "sink"
  val BatchProcessorType = "batch"
  val ExternalProcessorType = "external"
  val InputPortIngestionType = "input-port-ingestion"


  //  def resolveReadSchema(coreProperties: CoreProperties): Option[Schema] = {
  //    var schema = coreProperties.readSchema
  //
  //    if (schema.isEmpty) {
  //      schema = coreProperties.readSchemaId.flatMap(AvroSchemaStore.get)
  //    }
  //
  //    schema
  //  }
  //
  //  def resolveReadSchema(properties: JavaMap[String, String]): Option[Schema] = {
  //    resolveReadSchema(CoreProperties(properties.asScala.toMap))
  //  }
  //
  //  def resolveWriteSchema(coreProperties: CoreProperties, schemaId: Option[String]): Option[Schema] = {
  //    var schema = coreProperties.writeSchema
  //
  //    if(schema.isEmpty) {
  //      schema =  coreProperties.writeSchemaId.flatMap(AvroSchemaStore.get)
  //    }
  //
  //    if(schema.isEmpty) {
  //      schema = schemaId.flatMap(AvroSchemaStore.get)
  //    }
  //
  //    if(schema.isEmpty)
  //      resolveReadSchema(coreProperties)
  //    else
  //      schema
  //  }
  //
  //  def resolveWriteSchema(properties: JavaMap[String, String], schemaId: Option[String]): Option[Schema] = {
  //    resolveWriteSchema(CoreProperties(properties.asScala.toMap), schemaId)
  //  }

  def resolveSchemas(hasInput: Boolean,
                     properties: JavaMap[String, String],
                     className: String,
                     schemaId: String): (Option[Schema], Option[Schema]) = {
    val coreProperties: CoreProperties = CoreProperties(properties.asScala.toMap)

    val readSchema = coreProperties.resolveReadSchema
    if(hasInput && readSchema.isEmpty)
      throw new IllegalStateException("Read Schema for  " + className + " not available")


    val writeSchema = coreProperties.resolveWriteSchema(Option(schemaId))
    if(writeSchema.isEmpty)
      throw new IllegalStateException("Write Schema for  " + className + " not available")
    (readSchema, writeSchema)
  }

  def inputToGenericRecord(input: Array[Byte], readSchema: Option[Schema], writeSchema: Option[Schema]): Option[GenericRecord] = {

    Option(input).map(input => if(input.isEmpty) null else input.deSerToGenericRecord(readSchema, readSchema))
  }

  def resultToOutput(hasInput: Boolean,
                     out: Either[ErrorResponse, (String, AnyRef)],
                     readSchema: Option[Schema],
                     writeSchema: Option[Schema],
                     schemaId: String): Array[Array[Byte]] = {
    try {
      out match {
        case Left(error) =>  Array(RelationshipType.Failure.id.getBytes,
          error.serToBytes(Some(AvroSchemaStore.errorResponseSchema())))
        case Right(relRecord) => Array(relRecord._1.getBytes,
          // FIXME: This really needs to be optimised (to avoid double ser / deser)
          //        This is due to the fact that GenericRecord is essentially immutable
          //        We need to extend GenericRecord to allow for removing fields
          if(!hasInput || (schemaId != null && schemaId.nonEmpty))
            relRecord._2.serToBytes(writeSchema)
          else
            relRecord._2
              .serToBytes(readSchema)
              .deSerToGenericRecord(readSchema, writeSchema)
              .serToBytes(writeSchema))
      }
    } catch {
      case NonFatal(t) => resultError(t)
    }
  }

  def resultError(t: Throwable): Array[Array[Byte]] = Array(RelationshipType.Failure.id.getBytes,
    ErrorConstants.DCS306.withDescription(Option(t.getMessage).getOrElse(t.getClass.getName)).
      avroRecord().
      serToBytes(Some(AvroSchemaStore.errorResponseSchema())))

  def fromJsonPath(path: String, currentRecord: Option[GenericRecord]): Option[GenericRecordObject] = {
    fromJsonPath(path.split("\\.").toList, currentRecord)
  }

  def fromJsonPath(path: List[String], currentRecord: Option[GenericRecord]): Option[GenericRecordObject] = path match {
    case Nil => None
    case last :: Nil => currentRecord.map(r => GenericRecordObject(r, last))
    case "$" :: tail => currentRecord.flatMap(r => fromJsonPath(tail, Option(r)))
    case head :: tail => currentRecord.flatMap(r => fromJsonPath(tail, Option(r.get(head)).map(_.asInstanceOf[GenericRecord])))
  }
}

trait RemoteProcessor extends BaseProcessor
  with ProcessorDefinition
  with GlobalControl {

  import RemoteProcessor._

  def execute(record: Option[GenericRecord], properties: JavaMap[String, String]): List[Either[ErrorResponse, (String, AnyRef)]]

  def resolveSchemas(hasInput: Boolean, properties: JavaMap[String, String]): (Option[Schema], Option[Schema]) = {
    RemoteProcessor.resolveSchemas(hasInput, properties, className, schemaId)
  }

  def trigger(input: Array[Byte], properties: JavaMap[String, String]): Array[Array[Byte]] = {

    try {

      val (readSchema, writeSchema) = resolveSchemas(input.nonEmpty, properties)
      val in = inputToGenericRecord(input, readSchema, writeSchema)

      execute(in, properties).flatMap { out =>
        resultToOutput(input.nonEmpty, out, readSchema, writeSchema, schemaId)
      }.toArray
    } catch {
      case NonFatal(t) => resultError(t)
    }
  }

  def className: String = this.getClass.getName

  def resolveProperties(properties: util.Map[String, String]): util.Map[String, String] = _resolveProperties(properties.asScala.toMap).asJava

  def _resolveProperties(properties: Map[String, String]): Map[String, String] = properties

  override def schemaId: String = null

  // The methods 'resolveReadSchema' and 'resolveWriteSchema' should ideally be implemented
  // in the RemoteProcessor sub traits (Ingestion, Worker, ...)
  // implying that the method declared in the RemoteProcessor class should be public or protected
  // but since all public or protected the methods in Remote Processor are exposed over soap,
  // this method will also be unnecessarily exposed over soap. Hence the reason why this is a private method
  // in RemoteProcessor
  //  private def resolveReadSchema(coreProperties: CoreProperties): Option[Schema] = processorType match {
  //    case IngestionProcessorType => None
  //    case WorkerProcessorType | SinkProcessorType => RemoteProcessor.resolveReadSchema(coreProperties)
  //    case _ => throw new IllegalStateException("Unknown processor type : " + processorType)
  //  }
  //
  //  private def resolveWriteSchema(coreProperties: CoreProperties): Option[Schema] = processorType match {
  //    case IngestionProcessorType | WorkerProcessorType | SinkProcessorType => RemoteProcessor.resolveWriteSchema(coreProperties, Option(schemaId))
  //    case _ => throw new IllegalStateException("Unknown processor type : " + processorType)
  //  }


  def cast[T](obj: Object, fieldType: String): T =
    obj match {
      case no: Number => {
        val no: Number = obj.asInstanceOf[Number]
        fieldType match {
          case PropertyType.Double => no.doubleValue().asInstanceOf[T]
          case PropertyType.Int => no.intValue().asInstanceOf[T]
          case PropertyType.Long => no.longValue().asInstanceOf[T]
          case PropertyType.Float => no.longValue().asInstanceOf[T]
          case _ => no.asInstanceOf[T]
        }
      }
      case _ => obj.asInstanceOf[T]
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
      record.flatMap(r => Option(r.get(key))).map(_.toString)

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

    def fromJsonPath(path: String): Option[GenericRecordObject] = {
      fromJsonPath(path.split("\\.").toList)
    }

    def fromJsonPath(path: List[String]): Option[GenericRecordObject] = {
      RemoteProcessor.fromJsonPath(path, record)
    }

  }

  implicit class GenericRecordCasts(value: Option[Object]) {
    def asDouble: Option[Double] = value.map(_.asInstanceOf[Double])

    def asBoolean: Option[Boolean] = value.map(_.asInstanceOf[Boolean])

    def asInt: Option[Int] = value.map(_.asInstanceOf[Int])

    def asLong: Option[Long] = value.map(_.asInstanceOf[Long])

    def asFloat: Option[Float] = value.map(_.asInstanceOf[Float])

    def asString: Option[String] = value.map(_.toString)

    def asByteBuffer: Option[ByteBuffer] = value.map(_.asInstanceOf[ByteBuffer])

    def asCharSequence: Option[CharSequence] = value.map(_.asInstanceOf[CharSequence])

    def asGenericRecord: Option[GenericRecord] = value.map(_.asInstanceOf[GenericRecord])

    def asList[T]: Option[List[T]] = value.map(_.asInstanceOf[List[T]])

    def asMap[K, V]: Option[Map[K, V]] = value.map(_.asInstanceOf[util.HashMap[K, V]].asScala.toMap)
      .map(d => d.map(r => r._1.asInstanceOf[K] -> r._2.asInstanceOf[V]))

    def asGenericFixed: Option[GenericFixed] = value.map(_.asInstanceOf[GenericFixed])

  }

  implicit class GenericRecordObjectAccess(gro: Option[GenericRecordObject]) {
    def value: Option[Object] = gro.flatMap(r => r.value)
  }
}

trait ProcessorDefinition extends HasProperties
  with HasRelationships
  with HasConfiguration
  with HasMetaData {

  def details(): ProcessorDetails = ProcessorDetails(this.metadata(), this.configuration, this.relationships())
}



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

trait External extends RemoteProcessor {

  override def processorType(): String = RemoteProcessor.ExternalProcessorType

  override def configuration: Configuration = Configuration(inputMimeType = MediaType.OCTET_STREAM.toString,
    outputMimeType = MediaType.OCTET_STREAM.toString,
    processorClassName =  className,
    inputRequirementType = InputRequirementType.InputForbidden)

  override def trigger(input: Array[Byte], properties: JavaMap[String, String]): Array[Array[Byte]] =
    throw new UnsupportedOperationException

  override def execute(record: Option[GenericRecord],
                       propertyValues: util.Map[String, String]): List[Either[ErrorResponse, (String, GenericRecord)]] =
    throw new UnsupportedOperationException

  override def schemaId: String = className

  override def properties(): JavaList[RemoteProperty] = {
    val props = new util.ArrayList(super.properties())

    props.add(ExternalProcessorProperties.rootOutputConnectionIdProperty)
    props.add(ExternalProcessorProperties.outputPortNameProperty)
    props.add(ExternalProcessorProperties.receiverProperty)
    props.add(ExternalProcessorProperties.rootInputConnectionIdProperty)
    props.add(ExternalProcessorProperties.inputPortNameProperty)
    props.add(ExternalProcessorProperties.senderProperty)

    props
  }

}

trait InputPortIngestion extends RemoteProcessor {

  override def processorType(): String = RemoteProcessor.InputPortIngestionType

  override def configuration: Configuration = Configuration(inputMimeType = MediaType.OCTET_STREAM.toString,
    outputMimeType = MediaType.OCTET_STREAM.toString,
    processorClassName =  className,
    inputRequirementType = InputRequirementType.InputRequired)

  override def properties(): JavaList[RemoteProperty] = {
    val props = new util.ArrayList(super.properties())

    props.add(ExternalProcessorProperties.rootInputConnectionIdProperty)
    props.add(ExternalProcessorProperties.inputPortNameProperty)
    props.add(ExternalProcessorProperties.rootInputPortIdProperty)

    props
  }

  override final def _relationships(): Set[RemoteRelationship] = {
    Set(Success)
  }

  override final def execute(record: Option[GenericRecord], properties: util.Map[String, String]): List[Either[ErrorResponse, (String, AnyRef)]] = {
    List(Right(Success.id, record.get))
  }


  override final def schemaId: String = null

}




