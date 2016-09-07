package org.dcs.api.processor

import scala.beans.BeanProperty

/**
  * Created by cmathew on 31/08/16.
  */
trait HasConfiguration {

  def configuration: Configuration

}

case class Configuration(@BeanProperty var inputMimeType: String,
                         @BeanProperty var outputMimeType: String,
                         @BeanProperty var processorClassName: String,
                         @BeanProperty var stateful: Boolean = false,
                         @BeanProperty var triggerType: String = TriggerType.Default,
                         @BeanProperty var inputRequirementType: String = InputRequirementType.InputAllowed) {
  def this() = this("", "", "", false, TriggerType.Default, InputRequirementType.InputAllowed)
}

object TriggerType {
  val Default = "DEFAULT"
  val Serially = "SERIALLY"
  val WhenAnyDestinationAvailable = "WHEN_ANY_DESTINATION_AVAILABLE"
  val WhenEmpty = "WHEN_EMPTY"
}

object InputRequirementType {
  val InputRequired = "INPUT_REQUIRED"
  val InputAllowed = "INPUT_ALLOWED"
  val InputForbidden = "INPUT_FORBIDDEN"
}

