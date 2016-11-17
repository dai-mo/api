package org.dcs.api.service

import java.util.Date

import scala.beans.BeanProperty
import scala.concurrent.Future

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
                        @BeanProperty var state: String,
                        @BeanProperty var version: Long,
                        @BeanProperty var processors : List[ProcessorInstance],
                        @BeanProperty var connections: List[Connection]) {
  def this() = this("", "", "", "", 0.0.toLong, Nil, Nil)
}

case class FlowTemplate(@BeanProperty var id: String,
                        @BeanProperty var uri: String,
                        @BeanProperty var name: String,
                        @BeanProperty var description: String,
                        @BeanProperty var timestamp: Date) {
  def this() = this("", "", "", "", null)
}



trait FlowApiService {
  def templates(clientId: String): Future[List[FlowTemplate]]
  def instantiate(flowTemplateId: String, userId: String, authToken: String): Future[FlowInstance]
  def instance(flowInstanceId: String, userId: String, authToken: String): Future[FlowInstance]
  def instances(userId: String, authToken: String): Future[List[FlowInstance]]
  def start(flowInstanceId: String, userId: String, authToken: String): Future[Boolean]
  def stop(flowInstanceId: String, userId: String, authToken: String): Future[Boolean]
  def remove(flowInstanceId: String, userId: String, authToken: String): Future[Boolean]
}

// --- Flow Models/ API End ---


// --- Processor Models/ API Start ---

case class ProcessorInstance(@BeanProperty var id: String,
                             @BeanProperty var `type`: String,
                             @BeanProperty var status: String,
                             @BeanProperty var version: Long) {
  def this() = this("", "", "", 0.0.toLong)
}

case class ProcessorType(@BeanProperty var pType:String,
                         @BeanProperty var description:String,
                         @BeanProperty var tags: List[String]) {
  def this() = this("", "", Nil)
}

trait ProcessorApiService {
  def types(userId: String): Future[List[ProcessorType]]
  def typesSearchTags(str:String, userId: String): Future[List[ProcessorType]]
  def create(name: String, ptype: String, clientToken: String): Future[ProcessorInstance]
  def instance(processorId: String, userId: String): Future[ProcessorInstance]
  def start(processorId: String, userId: String): Future[ProcessorInstance]
  def stop(processorId: String, userId: String): Future[ProcessorInstance]
  def remove(processorId: String, userId: String): Future[Boolean]
}

// --- Processor Models/ API End ---


// --- Provenance Models/ API Start ---

case class Provenance(@BeanProperty var id: String,
                      @BeanProperty var queryId: String,
                      @BeanProperty var clusterNodeId: String,
                      @BeanProperty var content: String) {
  def this() = this("", "", "", "")
}

trait ProvenanceApiService {
  def provenance(processorId: String, processorType: String, maxResults: Int, startDate: Date, endDate: Date): Future[List[Provenance]]
}

// --- Provenance Models/ API End ---
