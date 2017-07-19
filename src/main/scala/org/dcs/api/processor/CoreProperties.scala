package org.dcs.api.processor


import org.apache.avro.Schema
import org.apache.avro.Schema.{Parser, Type}
import org.dcs.commons.error._
import org.dcs.commons.serde.AvroSchemaStore
import org.dcs.commons.serde.JsonSerializerImplicits._

import scala.collection.JavaConverters._

/**
  * Created by cmathew on 09.03.17.
  */
object CoreProperties {
  val ReadSchemaIdKey = "_READ_SCHEMA_ID"
  val WriteSchemaIdKey = "_WRITE_SCHEMA_ID"
  val ReadSchemaKey = "_READ_SCHEMA"
  val WriteSchemaKey = "_WRITE_SCHEMA"

  val FieldsToMapKey = "_FIELDS_TO_MAP"
  val FieldActionsKey = "_FIELDS_ACTIONS"

  val ProcessorTypeKey = "_PROCESSOR_TYPE"
  val ProcessorClassKey = "_PROCESSOR_CLASS"

  val SchemaNamespace = "org.dcs.processor"

  def remoteProperty(key: String,
                     description: String,
                     dValue: String,
                     isRequired: Boolean,
                     isDynamic: Boolean,
                     pLevel: Int,
                     values: Set[String] = Set()): RemoteProperty = {
    RemoteProperty(key,
      key,
      description,
      defaultValue = dValue,
      required = isRequired,
      dynamic = isDynamic,
      level = pLevel,
      possibleValues = values.map(v => PossibleValue(v, v, v)).asJava)
  }

  def apply(properties: Map[String, String]): CoreProperties =
    new CoreProperties(properties)

  def readSchemaIdProperty(defaultValue: String = null): RemoteProperty =
    remoteProperty(ReadSchemaIdKey,
      "Id of avro schema used to deserialise the input of this processor to a generic record [Level " + PropertyLevel.Internal + "]",
      defaultValue,
      isRequired = false,
      isDynamic = false,
      PropertyLevel.Internal)

  def writeSchemaIdProperty(defaultValue: String = null): RemoteProperty =
    remoteProperty(WriteSchemaIdKey,
      "Id of avro schema used to to serialise the output of this processor to a byte array [Level" + PropertyLevel.Internal + "]",
      defaultValue,
      isRequired = false,
      isDynamic = false,
      PropertyLevel.Internal)

  def readSchemaProperty(defaultValue: String = null): RemoteProperty =
    remoteProperty(ReadSchemaKey,
      "Avro read schema used to deserialise the byte array input of this processor to a generic record [Level" + PropertyLevel.Internal + "]",
      defaultValue,
      isRequired = false,
      isDynamic = true,
      PropertyLevel.Internal)

  def writeSchemaProperty(defaultValue: String = null): RemoteProperty =
    remoteProperty(WriteSchemaKey,
      "Avro schema used to serialise the output of this processor to a byte array [Level" + PropertyLevel.Internal + "]",
      defaultValue,
      isRequired = false,
      isDynamic = true,
      PropertyLevel.Internal)

  def fieldsToMapProperty(fields: Set[ProcessorSchemaField]): RemoteProperty =
    remoteProperty(FieldsToMapKey,
      "Field <> JsonPath Mappings for fields required by this processor [Level" + PropertyLevel.Internal + "]",
      fields.toJson,
      isRequired = true,
      isDynamic = false,
      PropertyLevel.Internal)

  def fieldActionsProperty(actions: Set[Action]): RemoteProperty =
    remoteProperty(FieldActionsKey,
      "A list of actions mapped to json paths which are executed by the processor [Level" + PropertyLevel.Internal + "]",
      actions.toJson,
      isRequired = true,
      isDynamic = false,
      PropertyLevel.Internal)

  def processorTypeProperty(defaultValue: String = null): RemoteProperty =
    remoteProperty(ProcessorTypeKey,
      "Type of processor [Level" + PropertyLevel.Internal + "]",
      defaultValue,
      isRequired = true,
      isDynamic = false,
      PropertyLevel.Internal)

}

class CoreProperties(properties: Map[String, String]) {

  import CoreProperties._

  val readSchemaId: Option[String] = properties.get(ReadSchemaIdKey).flatMap(schemaId => Option(if(schemaId == null || schemaId.isEmpty) null else schemaId))
  val writeSchemaId: Option[String] = properties.get(WriteSchemaIdKey).flatMap(schemaId => Option(if(schemaId == null || schemaId.isEmpty) null else schemaId))

  // FIXME: The actual schemas should not be required and should be removed
  //        once the transient schemaId -> schema store is setup
  val readSchema: Option[Schema] = properties.get(ReadSchemaKey).flatMap(schema => Option(if(schema == null || schema.isEmpty) null else (new Parser).parse(schema)))
  val writeSchema: Option[Schema] = properties.get(WriteSchemaKey).flatMap(schema => Option(if(schema == null || schema.isEmpty) null else (new Parser).parse(schema)))

  def resolveReadSchema(): Option[Schema] = {
    var schema = readSchema

    if (schema.isEmpty) {
      schema = readSchemaId.flatMap(AvroSchemaStore.get)
    }

    schema
  }


  def resolveWriteSchema(): Option[Schema] = {
    var schema = writeSchema

    if(schema.isEmpty) {
      schema =  writeSchemaId.flatMap(AvroSchemaStore.get)
    }

    if(schema.isEmpty)
      resolveReadSchema()
    else
      schema
  }
}
