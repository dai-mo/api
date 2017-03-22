package org.dcs.api.processor

import java.util.{Set => JavaSet}

import scala.beans.BeanProperty
import scala.collection.JavaConverters._

/**
  * Created by cmathew on 30/08/16.
  */
trait HasRelationships {

  def relationships(): JavaSet[RemoteRelationship] = (_relationships() + RelationshipType.FAILURE).asJava

  protected def _relationships(): Set[RemoteRelationship] = Set()

}


case class RemoteRelationship(@BeanProperty var id: String,
                              @BeanProperty var description: String) {
  def this() = this("", "")
}

object RelationshipType {
  val SucessRelationship = "success"
  val FailureRelationship = "failure"
  val UnknownRelationship = "unknown"

  val SUCCESS = RemoteRelationship(RelationshipType.SucessRelationship,
    "All status updates will be routed to this relationship")
  val FAILURE = RemoteRelationship(RelationshipType.FailureRelationship,
    "All failed updates will be routed to this relationship")
  val UNKNOWN = RemoteRelationship(RelationshipType.UnknownRelationship,
    "Represents an nknown Relationship")
}


