package org.dcs.api.processor

import org.dcs.api.Constants
import org.dcs.api.util.WithArgs

object ExternalProcessorProperties {

  val ReceiverKey = "_EXTERNAL_RECEIVER"
  val SenderKey = "_EXTERNAL_SENDER"

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
