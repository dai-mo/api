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

import java.util.{Set => JavaSet}

import scala.beans.BeanProperty
import scala.collection.JavaConverters._

/**
  * Created by cmathew on 30/08/16.
  */
trait HasRelationships {

  def relationships(): JavaSet[RemoteRelationship] = (_relationships() + RelationshipType.Failure).asJava

  protected def _relationships(): Set[RemoteRelationship] = Set()

}


case class RemoteRelationship(@BeanProperty var id: String,
                              @BeanProperty var description: String,
                              @BeanProperty var autoTerminate: Boolean = false) {
  def this() = this("", "", false)
}

object RelationshipType {
  val Invalid = RemoteRelationship("invalid",
    "All records with invalid values will be routed to this relationship",
    false)

  val Valid = RemoteRelationship("valid",
    "All records with valid values will be routed to this relationship",
    false)

  val Success = RemoteRelationship("success",
    "All status updates will be routed to this relationship",
    false)

  val Failure = RemoteRelationship("failure",
    "All failed updates will be routed to this relationship",
    false)

  val Unknown = RemoteRelationship( "unknown",
    "Represents an unknown Relationship",
    false)
}


