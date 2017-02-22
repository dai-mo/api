package org.dcs.api.data

import java.util.Date


/**
  * Created by cmathew on 02.02.17.
  */

object FlowData {

  def mapToString[K, V](map: Map[K, V]): String = {
    if(map == null || map.isEmpty)
      ""
    else
      map.toList.map(x => x._1 + ":" + x._2).mkString(",")
  }

  def listToString[T](list: List[T]): String = {
    if(list == null || list.isEmpty)
      ""
    else
      list.mkString(",")
  }

  def stringToMap(str: String): Map[String, String] = {
    if(str.isEmpty)
      Map[String, String]()
    else
      str.split(",").map(a => {
        val attr = a.split(":")
        attr.head -> attr.tail.head
      }).toMap
  }

  def stringToList(str: String): List[String] = {
    if(str.isEmpty)
      Nil
    else
      str.split(",").toList
  }

}

case class FlowDataContent(id: String, claimCount: Int, timestamp: Date, data: Array[Byte]) {
  override def equals(that: Any): Boolean = that match {
    case FlowDataContent(thatId, thatClaimCount, thatTimestamp, thatData) =>
      thatId == this.id &&
        thatClaimCount == this.claimCount &&
        thatTimestamp == this.timestamp &&
        thatData.deep == this.data.deep
    case _ => false
  }
}


case class FlowDataProvenance(id: String,
                              eventId: Double,
                              eventTime: Double,
                              flowFileEntryDate: Double,
                              lineageStartEntryDate: Double,
                              fileSize: Double,
                              previousFileSize: Double,
                              eventDuration: Double,
                              eventType: String,
                              attributes: String,
                              previousAttributes:String,
                              updatedAttributes: String,
                              componentId: String,
                              componentType: String,
                              transitUri: String,
                              sourceSystemFlowFileIdentifier: String,
                              flowFileUuid: String,
                              parentUuids: String,
                              childUuids: String,
                              alternateIdentifierUri: String,
                              details: String,
                              relationship: String,
                              sourceQueueIdentifier: String,
                              contentClaimIdentifier: String,
                              previousContentClaimIdentifier: String) {
  override def equals(that: Any): Boolean = {
    if(that == null)
      false
    else if(that.isInstanceOf[FlowDataProvenance]) {
      val thatFdp = that.asInstanceOf[FlowDataProvenance]
      this.eventId == thatFdp.eventId
    } else
      false
  }

}
