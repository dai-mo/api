package org.dcs.api.processor

import org.apache.avro.Schema
import org.dcs.api.processor.CoreProperties.ProcessorClassKey
import org.dcs.commons.error.{ErrorConstants, ValidationErrorResponse}

/**
  * Created by cmathew on 14.07.17.
  */
object ProcessorValidation {
  def schemaPathCheck(processorName: String, processorId: String, schema: Schema, properties: Map[String, String]): Option[ValidationErrorResponse] = {
    val validationInfo = properties.flatMap({
      case (CoreProperties.FieldsToMapKey, value) =>
        SchemaValidation.schemaPathCheck(processorName, processorId, schema, CoreProperties.FieldsToMapKey, value)
      case (CoreProperties.FieldActionsKey, value) =>
        SchemaValidation.schemaPathCheck(processorName, processorId, schema, CoreProperties.FieldActionsKey, value)
      case _ => Nil
    }).toList

    if(validationInfo.nonEmpty)
      Some(ErrorConstants.DCS306.withDescription("Invalid Processor Schema Field path").validation(validationInfo))
    else
      None
  }

  def hasWriteSchema(properties: Map[String, String]): Boolean =
    CoreProperties(properties).resolveWriteSchema().nonEmpty


  def validate(processorId: String,
               properties: Map[String, String],
               propertyDefinitions: List[RemoteProperty],
               checkAllPropertiesPresent: Boolean = true): Option[ValidationErrorResponse] = {
    val coreProperties = CoreProperties(properties)

    var validationInfo: List[Map[String, String]] = Nil

    val processorName = properties(ProcessorClassKey)

    val isIngestion = properties.get(CoreProperties.ProcessorTypeKey).map(t => t == RemoteProcessor.IngestionProcessorType).get

    var readSchema = coreProperties.resolveReadSchema()

    if(!isIngestion && readSchema.isEmpty)
      validationInfo = ValidationErrorResponse.processorSchemaValidation(ErrorConstants.DCS307, processorName, processorId) ::  validationInfo

    val writeSchema = coreProperties.resolveWriteSchema()

    if(writeSchema.isEmpty)
      validationInfo = ValidationErrorResponse.processorSchemaValidation(ErrorConstants.DCS308, processorName, processorId) ::  validationInfo
    else
      validationInfo = properties.flatMap({
        case (CoreProperties.FieldsToMapKey, value) => {
          SchemaValidation.schemaChecks(SchemaValidation.AllSchemaFieldsExists,
            processorName,
            processorId,
            readSchema,
            CoreProperties.FieldsToMapKey,
            value,
            propertyDefinitions.find(_.name == CoreProperties.FieldsToMapKey).get.defaultValue)
        }
        case (CoreProperties.FieldActionsKey, value) => {
          SchemaValidation.schemaChecks(SchemaValidation.AtLeastOneSchemaFieldExists,
            processorName,
            processorId,
            readSchema,
            CoreProperties.FieldActionsKey,
            value,
            propertyDefinitions.find(_.name == CoreProperties.FieldActionsKey).get.defaultValue) ++
            FieldActions.validate(value, processorName, processorId)
        }
        case _ => Nil
      }
      ).toList  ++ validationInfo

    validationInfo = (for {
      p <- properties.toList
      pd <- propertyDefinitions
      if pd.name == p._1 && pd.required && p._2.isEmpty
    } yield ValidationErrorResponse.processorPropertyValidation(ErrorConstants.DCS314,
      processorName,
      processorId,
      p._1,
      pd.`type`)) ++ validationInfo

    if(checkAllPropertiesPresent)
      validationInfo = propertyDefinitions
        .filter(pd => pd.required && !properties.exists(_._1 == pd.name))
        .map(pd => ValidationErrorResponse.processorPropertyValidation(ErrorConstants.DCS313,
          processorName,
          processorId,
          pd.name,
          pd.`type`))  ++ validationInfo

    // FIXME: Add validation for processor specific validators (via remote service)

    if(validationInfo.nonEmpty)
      Some(ErrorConstants.DCS306.withDescription("Invalid Processor Properties").validation(validationInfo))
    else
      None
  }

  def removeInvalid(properties: Map[String, String],
                    validationErrorResponse: ValidationErrorResponse): Map[String, String] = {
    properties.filter(p =>
      !validationErrorResponse.validationInfo
        .exists(_
          .exists(vp => vp._1 == ValidationErrorResponse.ProcessorPropertyName &&
            p._1 == vp._2)
        )
    )
  }
}
