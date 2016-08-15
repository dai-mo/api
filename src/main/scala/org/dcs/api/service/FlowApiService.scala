package org.dcs.api.service

import java.util.Date

import scala.beans.BeanProperty

/**
  * Created by cmathew on 05/06/16.
  */

// --- Flow Models/ API Start ---

case class ConnectionPort(@BeanProperty var id: String,
                          @BeanProperty var `type`: String) {
  def this() = this("", "")
}

case class Connection(@BeanProperty var id: String,
                      @BeanProperty var source: ConnectionPort,
                      @BeanProperty var destination: ConnectionPort) {
  def this() = this("", ConnectionPort("", ""), ConnectionPort("", ""))
}

case class FlowInstance(@BeanProperty var id: String,
                        @BeanProperty var name: String,
                        @BeanProperty var nameId: String,
                        @BeanProperty var version: Long,
                        @BeanProperty var processors : List[ProcessorInstance],
                        @BeanProperty var connections: List[Connection]) {
  def this() = this("", "", "", 0.0.toLong, Nil, Nil)
}

case class FlowTemplate(@BeanProperty var id: String,
                        @BeanProperty var uri: String,
                        @BeanProperty var name: String,
                        @BeanProperty var description: String,
                        @BeanProperty var timestamp: Date) {
  def this() = this("", "", "", "", null)
}



trait FlowApiService {
  def templates(clientId: String):List[FlowTemplate]
  def instantiate(flowTemplateId: String, userId: String, authToken: String):FlowInstance
  def instance(flowInstanceId: String, userId: String, authToken: String): FlowInstance
  def instances(userId: String, authToken: String): List[FlowInstance]
  def start(flowInstanceId: String, userId: String, authToken: String): Boolean
  def stop(flowInstanceId: String, userId: String, authToken: String): Boolean
  def remove(flowInstanceId: String, userId: String, authToken: String): Boolean
}

// --- Flow Models/ API End ---


// --- Processor Models/ API Start ---

case class ProcessorInstance(@BeanProperty var id: String,
                             @BeanProperty var status: String,
                             @BeanProperty var version: Long) {
  def this() = this("", "", 0.0.toLong)
}

case class ProcessorType(@BeanProperty var pType:String,
                         @BeanProperty var description:String,
                         @BeanProperty var tags: List[String]) {
  def this() = this("", "", Nil)
}

trait ProcessorApiService {
  def types(userId: String): List[ProcessorType]
  def typesSearchTags(str:String, userId: String): List[ProcessorType]
  def create(name: String, ptype: String, clientToken: String): ProcessorInstance
  def instance(processorId: String, userId: String): ProcessorInstance
  def start(processorId: String, userId: String): ProcessorInstance
  def stop(processorId: String, userId: String): ProcessorInstance
  def remove(processorId: String, userId: String): Boolean
}

// --- Processor Models/ API End ---


// --- Provenance Models/ API Start ---

case class Provenance(@BeanProperty var id: String,
                      @BeanProperty var queryId: String,
                      @BeanProperty var content: String) {
  def this() = this("", "", "")
}

trait ProvenanceApiService {
  def provenance(processorId: String, maxResults: Int, startDate: Date, endDate: Date): List[Provenance]
}

// --- Provenance Models/ API End ---
