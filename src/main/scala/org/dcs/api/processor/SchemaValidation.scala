/*
 * Copyright (c) 2017-2018 brewlabs SAS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.dcs.api.processor

import org.apache.avro.Schema
import org.dcs.commons.SchemaField
import org.dcs.commons.error.{ErrorConstants, ValidationErrorResponse}
import org.dcs.commons.serde.JsonSerializerImplicits._

/**
  * Created by cmathew on 18.07.17.
  */
object SchemaValidation {

  val AllSchemaFieldsExists = "AllSchemaFieldsExists"
  val AtLeastOneSchemaFieldExists = "AtLeastOneSchemaFieldExists"

  def schemaFieldsExistsCheck(existsCheckType: String,
                              processorName: String,
                              processorId: String,
                              schema: Schema,
                              schemaPropertyName: String,
                              processorFields: String,
                              defaultProcessorFields: String): List[Map[String, String]] = {
    existsCheckType match {
      case AllSchemaFieldsExists => schemaFieldsAllExistsCheck(processorName,
        processorId,
        schema,
        schemaPropertyName,
        processorFields,
        defaultProcessorFields)
      case AtLeastOneSchemaFieldExists => schemaFieldsAtLeastOneExistsCheck(processorName,
        processorId,
        schema,
        schemaPropertyName,
        processorFields,
        defaultProcessorFields)
      case _ => Nil
    }
  }


  def schemaFieldsAllExistsCheck(processorName: String,
                                 processorId: String,
                                 schema: Schema,
                                 schemaPropertyName: String,
                                 processorFields: String,
                                 defaultProcessorFields: String): List[Map[String, String]] = {
    if(processorFields.nonEmpty) {
      val ftm = processorFields.asList[ProcessorSchemaField]
      defaultProcessorFields.asList[ProcessorSchemaField]
        .filter(f => !ftm.exists(_.name == f.name))
        .map(f =>
          ValidationErrorResponse
            .processorSchemaFieldValidation(ErrorConstants.DCS309,
              processorName,
              processorId,
              schemaPropertyName,
              f.name,
              f.jsonPath,
              f.fieldType))
    } else Nil
  }

  def schemaFieldsAtLeastOneExistsCheck(processorName: String,
                                        processorId: String,
                                        schema: Schema,
                                        schemaPropertyName: String,
                                        processorFields: String,
                                        defaultProcessorFields: String): List[Map[String, String]] = {
    if(processorFields.nonEmpty) {
      val ftm = processorFields.asList[ProcessorSchemaField]
      defaultProcessorFields.asList[ProcessorSchemaField]
        .find(f => ftm.exists(_.name == f.name))
        .map(f => Nil)
        .getOrElse(List(ValidationErrorResponse
          .processorSchemaFieldValidation(ErrorConstants.DCS309,
            processorName,
            processorId,
            schemaPropertyName,
            "",
            "",
            "")))
    } else Nil
  }


  def schemaPathCheck(processorName: String,
                      processorId: String,
                      schema: Schema,
                      schemaPropertyName: String,
                      processorFields: String): List[Map[String, String]] = {
    if(processorFields.nonEmpty) {
      processorFields.asList[ProcessorSchemaField]
        .filter(f => f.jsonPath.nonEmpty && !SchemaField.validatePath(schema, f.jsonPath))
        .map(f => ValidationErrorResponse
          .processorSchemaFieldValidation(ErrorConstants.DCS310,
            processorName,
            processorId,
            schemaPropertyName,
            f.name,
            f.jsonPath,
            f.fieldType))
    } else Nil
  }


  def schemaFieldTypeCheck(processorName: String,
                           processorId: String,
                           schema: Schema,
                           schemaPropertyName: String,
                           processorFields: String): List[Map[String, String]] = {
    if(processorFields.nonEmpty) {
      processorFields.asList[ProcessorSchemaField]
        .filter(f => f.fieldType.nonEmpty &&
          SchemaField.find(schema, f.jsonPath).exists(sf => {
            val schemaType = sf.schema().getType.getName
            if (schemaType == PropertyType.Union)
              sf.schema().getTypes.get(1).getType.getName != f.fieldType
            else if(f.fieldType == PropertyType.Double)
              schemaType != PropertyType.Int &&
                schemaType != PropertyType.Float &&
                schemaType != PropertyType.Long &&
                schemaType != PropertyType.Double
            else
              sf.schema().getType.getName != f.fieldType
          }))
        .map(f => ValidationErrorResponse
          .processorSchemaFieldValidation(ErrorConstants.DCS311,
            processorName,
            processorId,
            schemaPropertyName,
            f.name,
            f.jsonPath,
            f.fieldType))
    } else Nil
  }

  def schemaFieldEmptyCheck(processorName: String,
                            processorId: String,
                            schema: Schema,
                            schemaPropertyName: String,
                            processorFields: String): List[Map[String, String]] = {
    if(processorFields.nonEmpty) {
      processorFields.asList[ProcessorSchemaField]
        .filter(f => f.jsonPath.isEmpty || f.fieldType.isEmpty)
        .map(f => ValidationErrorResponse
          .processorSchemaFieldValidation(ErrorConstants.DCS312,
            processorName,
            processorId,
            schemaPropertyName,
            f.name,
            f.jsonPath,
            f.fieldType))
    } else Nil
  }

  def schemaChecks(existsCheckType: String,
                   processorName: String,
                   processorId: String,
                   schema: Option[Schema],
                   schemaPropertyName: String,
                   fieldsToMap: String,
                   defaultFieldsToMap: String): List[Map[String, String]] = {
    schema.map(s => schemaFieldsExistsCheck(existsCheckType,
      processorName,
      processorId,
      s,
      schemaPropertyName,
      fieldsToMap,
      defaultFieldsToMap) ++ schemaPathCheck(processorName: String,
      processorId,
      s,
      schemaPropertyName,
      fieldsToMap) ++ schemaFieldTypeCheck(processorName,
      processorId,
      s,
      schemaPropertyName,
      fieldsToMap)  ++ schemaFieldEmptyCheck(processorName,
      processorId,
      s,
      schemaPropertyName,
      fieldsToMap: String) ++ Nil).getOrElse(Nil)
  }
}
