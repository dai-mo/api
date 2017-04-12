package org.dcs.api.service

import java.util
import java.util.Date

import org.dcs.api.processor.RemoteProcessor

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
  def templates(): Future[List[FlowTemplate]]
  def create(flowName: String): Future[FlowInstance]
  def instantiate(flowTemplateId: String): Future[FlowInstance]
  def instance(flowInstanceId: String): Future[FlowInstance]
  def instances(): Future[List[FlowInstance]]
  def start(flowInstanceId: String): Future[Boolean]
  def stop(flowInstanceId: String): Future[Boolean]
  def remove(flowInstanceId: String): Future[Boolean]
}

// --- Flow Models/ API End ---


// --- Processor Models/ API Start ---

case class ProcessorInstance(@BeanProperty var id: String,
                             @BeanProperty var name: String,
                             @BeanProperty var `type`: String,
                             @BeanProperty var processorType: String,
                             @BeanProperty var status: String,
                             @BeanProperty var version: Long,
                             @BeanProperty var properties: Map[String, String]) {
  def this() = this("", "", "", "", "", 0.0.toLong, Map())
}

case class ProcessorType(@BeanProperty var pType:String,
                         @BeanProperty var description:String,
                         @BeanProperty var tags: List[String]) {
  def this() = this("", "", Nil)
}

case class ProcessorServiceDefinition(@BeanProperty var processorServiceClassName: String,
                                      @BeanProperty var processorType: String,
                                      @BeanProperty var stateful: Boolean) {
  def this() = this("", RemoteProcessor.WorkerProcessorType, false)
}

trait ProcessorApiService {
  def types(userId: String): Future[List[ProcessorType]]
  def typesSearchTags(str:String, userId: String): Future[List[ProcessorType]]
  def create(processorServiceDefinition: ProcessorServiceDefinition, processGroupId: String): Future[ProcessorInstance]
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
                      @BeanProperty var raw: Array[Byte],
                      @BeanProperty var content: String,
                      @BeanProperty var timestamp: Date,
                      @BeanProperty var relationship: String) {
  def this() = this("", "", "", Array[Byte](), "", null, "")
}

trait ProvenanceApiService {
  def provenance(processorId: String, processorType: String, maxResults: Int, startDate: Date, endDate: Date): Future[List[Provenance]]
}

// --- Provenance Models/ API End ---

// --- Flow Data Models / API start ---

trait IFlowDataService {
  def provenanceByComponentId(cid: String, maxResults: Int): util.List[Provenance]
}

// --- Flow Data Models / API end ---
