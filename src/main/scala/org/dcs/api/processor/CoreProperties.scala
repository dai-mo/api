package org.dcs.api.processor

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser


/**
  * Created by cmathew on 09.03.17.
  */
object CoreProperties {
  val SchemaIdKey = "_SCHEMA_ID"
  val SchemaKey = "_SCHEMA"
  val FieldToMapKey = "_FIELD_TO_MAP"
  val SchemaNamespace = "org.dcs.processor"

  def apply(properties: Map[String, String]): CoreProperties =
    new CoreProperties(properties)
}

class CoreProperties(properties: Map[String, String]) {
  import CoreProperties._

  private val parser = new Parser

  val schema: Option[Schema] = properties.get(SchemaKey).map(parser.parse)
  val schemaId: Option[String] = properties.get(SchemaIdKey)
}
