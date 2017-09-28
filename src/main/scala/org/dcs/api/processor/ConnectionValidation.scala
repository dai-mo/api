package org.dcs.api.processor

import org.dcs.api.service.{ConnectionConfig, FlowComponent}
import org.dcs.commons.error.ErrorConstants

object ConnectionValidation {

  def validate(connectionConfig: ConnectionConfig): Unit = {
    (connectionConfig.source.componentType, connectionConfig.destination.componentType) match {
      case (FlowComponent.ProcessorType, FlowComponent.ProcessorType) |
           (FlowComponent.ProcessorType, FlowComponent.ExternalProcessorType) |
           (FlowComponent.ExternalProcessorType, FlowComponent.ProcessorType) => validateProcessorConnection(connectionConfig)
      case _ => exception(connectionConfig.source.componentType, connectionConfig.destination.componentType)
    }

  }

  def exception(sourceType: String, destinationType: String) =
    ErrorConstants.DCS315
      .withDescription("Cannot connect processor of type " + sourceType + " to processor of type " + destinationType)
      .exception()

  def  validateProcessorConnection(connectionConfig: ConnectionConfig): Unit = {
    val sourceProcessorType = connectionConfig.source.properties.get(CoreProperties.ProcessorTypeKey)
    val destinationProcessorType = connectionConfig.destination.properties.get(CoreProperties.ProcessorTypeKey)

    def exception(sourceType: String, destinationType: String) =
      ErrorConstants.DCS315
        .withDescription("Cannot connect processor of type " + sourceType + " to processor of type " + destinationType)
        .exception()

    if(sourceProcessorType.isDefined && destinationProcessorType.isDefined) {
      (sourceProcessorType.get, destinationProcessorType.get) match  {
        case (source, RemoteProcessor.IngestionProcessorType) =>
          throw exception(source, RemoteProcessor.IngestionProcessorType)
        case (RemoteProcessor.SinkProcessorType, destination) =>
          throw exception(RemoteProcessor.SinkProcessorType, destination)
        case _ => // do nothing
      }
    } else
      throw ErrorConstants.DCS316.withDescription("Source Processor Type or Destination Processor Type not available")
        .exception()
  }

}
