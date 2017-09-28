package org.dcs.api.util

import java.util.UUID

object NameId {

  val NameIdSep = ";"

  def apply(name: String): String = {
    name + NameIdSep + UUID.randomUUID().toString
  }

  def apply(name: String, id: String): String = {
    name + NameIdSep + id
  }

  def extractFromName(comments: String): (String, String) = {
    val idName = comments.split(NameIdSep)
    (idName(0), idName(1))
  }

}
