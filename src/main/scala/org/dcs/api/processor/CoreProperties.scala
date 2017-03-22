package org.dcs.api.processor

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser


/**
  * Created by cmathew on 09.03.17.
  */
object CoreProperties {
  val ReadSchemaIdKey = "_READ_SCHEMA_ID"
  val WriteSchemaIdKey = "_WRITE_SCHEMA_ID"
  val ReadSchemaKey = "_READ_SCHEMA"
  val WriteSchemaKey = "_WRITE_SCHEMA"
  val FieldToMapKey = "_FIELD_TO_MAP"
  val SchemaNamespace = "org.dcs.processor"

  def remoteProperty(key: String, defaultValue: String = null): RemoteProperty = {
    RemoteProperty(key, key, "", defaultValue, required = true)
  }

  def apply(properties: Map[String, String]): CoreProperties =
    new CoreProperties(properties)
}

class CoreProperties(properties: Map[String, String]) {
  import CoreProperties._

  private val parser = new Parser

  val readSchemaId: Option[String] = properties.get(ReadSchemaIdKey).flatMap(schemaId => Option(if(schemaId == null || schemaId.isEmpty) null else schemaId))
  val writeSchemaId: Option[String] = properties.get(WriteSchemaIdKey).flatMap(schemaId => Option(if(schemaId == null || schemaId.isEmpty) null else schemaId))

  // FIXME: The actual schemas should not be required and should be removed
  //        once the transient schemaId -> schema store is setup
  val readSchema: Option[Schema] = properties.get(ReadSchemaKey).map(schema => if(schema == null || schema.isEmpty) null else parser.parse(schema))
  val writeSchema: Option[Schema] = properties.get(WriteSchemaKey).map(schema => if(schema == null || schema.isEmpty) null else parser.parse(schema))

}
