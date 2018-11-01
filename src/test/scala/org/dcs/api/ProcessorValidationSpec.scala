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

package org.dcs.api

import org.dcs.api.TestFieldActionsProcessor.{ContainsCmd, StartsWithCmd}
import org.dcs.api.TestFieldsToMapProcessor.{FirstNameKey, LastNameKey, MiddleNameKey}
import org.dcs.api.processor.{Action, CoreProperties, _}
import org.dcs.commons.error.{ErrorConstants, ValidationErrorResponse}
import org.dcs.commons.serde.AvroSchemaStore
import org.dcs.commons.serde.JsonSerializerImplicits._

/**
  * Created by cmathew on 18.07.17.
  */
class ProcessorValidationSpec extends ApiUnitWordSpec {

  val TestClassName = "org.dcs.api.TestIngestionProcessor"
  val SchemaId = "org.dcs.api.Person"
  AvroSchemaStore.add(SchemaId, this.getClass.getResourceAsStream("/avro/" + SchemaId + ".avsc"))

  val ftmFields = Set(ProcessorSchemaField(FirstNameKey, PropertyType.String),
    ProcessorSchemaField(MiddleNameKey, PropertyType.String),
    ProcessorSchemaField(LastNameKey, PropertyType.String))

  val faFields = Set(Action(ContainsCmd, PropertyType.String),
    Action(StartsWithCmd, PropertyType.String))

  val ftmrp = CoreProperties.fieldsToMapProperty(ftmFields)
  val farp = CoreProperties.fieldActionsProperty(faFields)

  "Processor Validation" should {

    "return error when processor is not of type ingestion and no read schema is provided" in {
      val noReadSchemaPropertyMap =
        Map(
          CoreProperties.ProcessorClassKey -> TestClassName,
          CoreProperties.ProcessorTypeKey -> RemoteProcessor.WorkerProcessorType
        )
      val ver = ProcessorValidation.validate("", noReadSchemaPropertyMap, Nil).get
      assert(ver.validationInfo.size == 2)
      assert(ver.validationInfo.exists(_(ValidationErrorResponse.ErrorCode) == ErrorConstants.DCS307.code))
      assert(ver.validationInfo.exists(_(ValidationErrorResponse.ErrorCode) == ErrorConstants.DCS308.code))
    }

    "return error when processor is type ingestion and no write schema is provided" in {
      val noWriteSchemaPropertyMap =
        Map(
          CoreProperties.ProcessorClassKey -> TestClassName,
          CoreProperties.ProcessorTypeKey -> RemoteProcessor.IngestionProcessorType
        )
      val ver = ProcessorValidation.validate("", noWriteSchemaPropertyMap, Nil).get
      assert(ver.validationInfo.size == 1)
      assert(ver.validationInfo.exists(_(ValidationErrorResponse.ErrorCode) == ErrorConstants.DCS308.code))
    }

    "return error when processor schema field does not exist" in {
      var schemaFieldDoesNotExistPropertyMap =
        Map(
          CoreProperties.ProcessorClassKey -> TestClassName,
          CoreProperties.ReadSchemaIdKey -> SchemaId,
          CoreProperties.ProcessorTypeKey -> RemoteProcessor.WorkerProcessorType,
          CoreProperties.FieldsToMapKey ->
            List(
              ProcessorSchemaField(MiddleNameKey, PropertyType.String, "$.middle_name"),
              ProcessorSchemaField(LastNameKey, PropertyType.String, "$.last_name")
            ).toJson
        )
      var ver = ProcessorValidation.validate("", schemaFieldDoesNotExistPropertyMap, List(ftmrp)).get
      assert(ver.validationInfo.size == 1)
      assert(ver.validationInfo.exists(_ (ValidationErrorResponse.ErrorCode) == ErrorConstants.DCS309.code))

    }

    "return no error when at least one processor schema field exists" in {
      val schemaFieldDoesNotExistPropertyMap =
        Map(
          CoreProperties.ProcessorClassKey -> TestClassName,
          CoreProperties.ReadSchemaIdKey -> SchemaId,
          CoreProperties.ProcessorTypeKey -> RemoteProcessor.WorkerProcessorType,
          CoreProperties.FieldActionsKey ->
            List(
              Action(ContainsCmd, PropertyType.String, "$.first_name", "Ob")
            ).toJson
        )
      assert(ProcessorValidation.validate("", schemaFieldDoesNotExistPropertyMap, List(farp)).isEmpty)
    }

    "return error when specific processor schema validation fails" in {
      val schemaFieldDoesNotExistPropertyMap =
        Map(
          CoreProperties.ProcessorClassKey -> TestClassName,
          CoreProperties.ReadSchemaIdKey -> SchemaId,
          CoreProperties.ProcessorTypeKey -> RemoteProcessor.WorkerProcessorType,
          CoreProperties.FieldActionsKey ->
            List(
              Action(ContainsCmd, PropertyType.String, "$.first_name", "")
            ).toJson
        )
      val ver = ProcessorValidation.validate("", schemaFieldDoesNotExistPropertyMap, List(farp)).get
      assert(ver.validationInfo.size == 1)
      assert(ver.validationInfo.exists(_(ValidationErrorResponse.ErrorCode) == ErrorConstants.DCS314.code))
    }

    "return error when processor schema field path is invalid " in {
      val invalidSchemaFieldPathPropertyMap =
        Map(
          CoreProperties.ProcessorClassKey -> TestClassName,
          CoreProperties.ReadSchemaIdKey -> SchemaId,
          CoreProperties.ProcessorTypeKey -> RemoteProcessor.WorkerProcessorType,
          CoreProperties.FieldsToMapKey ->
            List(
              ProcessorSchemaField(FirstNameKey, PropertyType.String, "$.some.first_name"),
              ProcessorSchemaField(MiddleNameKey, PropertyType.String, "$.middle_name"),
              ProcessorSchemaField(LastNameKey, PropertyType.String, "$.last_name")
            ).toJson
        )
      val ver = ProcessorValidation.validate("", invalidSchemaFieldPathPropertyMap, List(ftmrp)).get
      assert(ver.validationInfo.size == 1)
      assert(ver.validationInfo.exists(_(ValidationErrorResponse.ErrorCode) == ErrorConstants.DCS310.code))
    }

    "return error when processor schema field type is invalid " in {
      val invalidSchemaFieldTypePropertyMap =
        Map(
          CoreProperties.ProcessorClassKey -> TestClassName,
          CoreProperties.ReadSchemaIdKey -> SchemaId,
          CoreProperties.ProcessorTypeKey -> RemoteProcessor.WorkerProcessorType,
          CoreProperties.FieldsToMapKey ->
            List(
              ProcessorSchemaField(FirstNameKey, PropertyType.Number, "$.first_name"),
              ProcessorSchemaField(MiddleNameKey, PropertyType.Number, "$.middle_name"),
              ProcessorSchemaField(LastNameKey, PropertyType.String, "$.last_name")
            ).toJson
        )
      val ver = ProcessorValidation.validate("", invalidSchemaFieldTypePropertyMap, List(ftmrp)).get
      assert(ver.validationInfo.size == 2)
      assert(ver.validationInfo.exists(_(ValidationErrorResponse.ErrorCode) == ErrorConstants.DCS311.code))
    }

    "return error when processor schema field type is empty " in {
      val invalidSchemaFieldEmptyPropertyMap =
        Map(
          CoreProperties.ProcessorClassKey -> TestClassName,
          CoreProperties.ReadSchemaIdKey -> SchemaId,
          CoreProperties.ProcessorTypeKey -> RemoteProcessor.WorkerProcessorType,
          CoreProperties.FieldsToMapKey ->
            List(
              ProcessorSchemaField(FirstNameKey, PropertyType.String, ""),
              ProcessorSchemaField(MiddleNameKey, PropertyType.String, "$.middle_name"),
              ProcessorSchemaField(LastNameKey, PropertyType.String, "$.last_name")
            ).toJson
        )
      val ver = ProcessorValidation.validate("", invalidSchemaFieldEmptyPropertyMap, List(ftmrp)).get
      assert(ver.validationInfo.size == 1)
      assert(ver.validationInfo.exists(_(ValidationErrorResponse.ErrorCode) == ErrorConstants.DCS312.code))
    }

    "return error when required processor property is empty" in {
      val missingRequiredPropertyMap =
        Map(
          CoreProperties.ProcessorClassKey -> TestClassName,
          CoreProperties.ReadSchemaIdKey -> SchemaId,
          CoreProperties.ProcessorTypeKey -> RemoteProcessor.WorkerProcessorType,
          CoreProperties.FieldsToMapKey -> ""
        )
      val ver = ProcessorValidation.validate("", missingRequiredPropertyMap, List(ftmrp)).get
      assert(ver.validationInfo.size == 1)
      assert(ver.validationInfo.exists(_(ValidationErrorResponse.ErrorCode) == ErrorConstants.DCS314.code))
    }

    "return error when required processor property is missing" in {
      val missingRequiredPropertyMap =
        Map(
          CoreProperties.ProcessorClassKey -> TestClassName,
          CoreProperties.ReadSchemaIdKey -> SchemaId,
          CoreProperties.ProcessorTypeKey -> RemoteProcessor.WorkerProcessorType
        )
      val ver = ProcessorValidation.validate("", missingRequiredPropertyMap, List(ftmrp)).get
      assert(ver.validationInfo.size == 1)
      assert(ver.validationInfo.exists(_(ValidationErrorResponse.ErrorCode) == ErrorConstants.DCS313.code))
    }

    "remove invalid properties " in {
      val invalidPropertyMap =
        Map(
          CoreProperties.ProcessorClassKey -> TestClassName,
          CoreProperties.ReadSchemaIdKey -> SchemaId,
          CoreProperties.ProcessorTypeKey -> RemoteProcessor.WorkerProcessorType,
          CoreProperties.FieldsToMapKey ->
            List(
              ProcessorSchemaField(FirstNameKey, PropertyType.String, "$.first_name"),
              ProcessorSchemaField(MiddleNameKey, PropertyType.String, "$.middle_name"),
              ProcessorSchemaField(LastNameKey, PropertyType.String, "$.last_name")
            ).toJson,
            CoreProperties.FieldActionsKey -> ""
        )
      val ver = ProcessorValidation.validate("", invalidPropertyMap, List(ftmrp, farp)).get
      assert(ver.validationInfo.size == 1)
      assert(invalidPropertyMap.size == 5)

      val validProperties = ProcessorValidation.removeInvalid(invalidPropertyMap, ver)
      assert(validProperties.size == 4)
      assert(!validProperties.exists(_._1 == CoreProperties.FieldActionsKey))
    }

  }

}
