package org.dcs.api.processor

import org.dcs.api.Constants
import org.dcs.api.processor.CoreProperties.remoteProperty
import org.dcs.api.util.WithArgs

object ExternalProcessorProperties {

  val ReceiverKey = "_EXTERNAL_RECEIVER"
  val SenderKey = "_EXTERNAL_SENDER"

  val RootInputConnectionIdKey = "_ROOT_INPUT_CONNECTION_ID"
  val RootOutputConnectionIdKey = "_ROOT_OUTPUT_CONNECTION_ID"

  val InputPortNameKey = "_INPUT_PORT_NAME"
  val OutputPortNameKey = "_OUTPUT_PORT_NAME"

  val NifiUrlKey = "nifiUrl"
  val NifiPortName = "portName"

  val HasExternal = "hasExternal"

  def nifiReceiverWithArgs(nifiApiBaseUrl: String, outputPortName: String): String = {
    WithArgs(Constants.NifiSparkReceiverClassName,
      List(NifiUrlKey -> nifiApiBaseUrl, NifiPortName -> outputPortName))
      .toString()
  }

  def nifiSenderWithArgs(nifiApiBaseUrl: String, inputPortName: String): String = {
    WithArgs(Constants.NifiSparkSenderClassName,
      List(NifiUrlKey -> nifiApiBaseUrl, NifiPortName -> inputPortName))
      .toString()
  }

  def rootOutputConnectionIdProperty =  remoteProperty(ExternalProcessorProperties.RootOutputConnectionIdKey,
    "Id of root output port [Level" + PropertyLevel.Open + "]",
    "",
    isRequired = true,
    isDynamic = false,
    PropertyLevel.Open)

  def outputPortNameProperty =  remoteProperty(ExternalProcessorProperties.OutputPortNameKey,
    "Name of flow instance output port [Level" + PropertyLevel.Open + "]",
    "",
    isRequired = true,
    isDynamic = false,
    PropertyLevel.Open)

  def receiverProperty =  remoteProperty(ExternalProcessorProperties.ReceiverKey,
    "Id of receiver for external processor [Level" + PropertyLevel.Open + "]",
    "",
    isRequired = true,
    isDynamic = false,
    PropertyLevel.Open)

  def rootInputConnectionIdProperty =  remoteProperty(ExternalProcessorProperties.RootInputConnectionIdKey,
    "Id of root input port [Level" + PropertyLevel.Open + "]",
    "",
    isRequired = true,
    isDynamic = false,
    PropertyLevel.Open)

  def inputPortNameProperty =  remoteProperty(ExternalProcessorProperties.InputPortNameKey,
    "Name of flow instance input port [Level" + PropertyLevel.Open + "]",
    "",
    isRequired = true,
    isDynamic = false,
    PropertyLevel.Open)

  def senderProperty =  remoteProperty(ExternalProcessorProperties.SenderKey,
    "Id of sender for external processor [Level" + PropertyLevel.Open + "]",
    "",
    isRequired = true,
    isDynamic = false,
    PropertyLevel.Open)
}
