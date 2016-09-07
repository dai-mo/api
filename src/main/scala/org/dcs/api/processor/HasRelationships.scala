package org.dcs.api.processor

import java.util.{Set => JavaSet}

import scala.beans.BeanProperty

/**
  * Created by cmathew on 30/08/16.
  */
trait HasRelationships {

  def relationships(): JavaSet[RemoteRelationship]

}

case class RemoteRelationship(@BeanProperty var name: String,
                              @BeanProperty var description: String) {
  def this() = this("", "")
}

object RelationshipType {
  val SucessRelationship = "success"
  val FailureRelationship = "failure"
}


