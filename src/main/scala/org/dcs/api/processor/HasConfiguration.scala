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

