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


