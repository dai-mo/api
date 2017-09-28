package org.dcs.api.processor

import org.dcs.api.Constants
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

  def nifiReceiverWithArgs(nifiApiBaseUrl: String, outputPortName: String): String = {
    WithArgs(Constants.NifiSparkReceiverClassName,
      Map(NifiUrlKey -> nifiApiBaseUrl, NifiPortName -> outputPortName))
      .toString()
  }

  def nifiSenderWithArgs(nifiApiBaseUrl: String, inputPortName: String): String = {
    WithArgs(Constants.NifiSparkSenderClassName,
      Map(NifiUrlKey -> nifiApiBaseUrl, NifiPortName -> inputPortName))
      .toString()
  }


}
