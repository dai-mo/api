package org.dcs.api.service

import java.util
import java.util.Date

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonInclude.Include
import org.dcs.api.processor._
import org.dcs.commons.SchemaAction

import scala.beans.BeanProperty
import scala.concurrent.Future

/**
  * Created by cmathew on 05/06/16.
  */

// --- Flow Models/ API Start ---

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
  def create(flowName: String, clientId: String): Future[FlowInstance]
  def instantiate(flowTemplateId: String, clientId: String): Future[FlowInstance]
  def instance(flowInstanceId: String): Future[FlowInstance]
  def instances(): Future[List[FlowInstance]]
  def start(flowInstanceId: String): Future[FlowInstance]
  def stop(flowInstanceId: String): Future[FlowInstance]
  def remove(flowInstanceId: String, version: Long, clientId: String): Future[Boolean]
}

// --- Flow Models/ API End ---


// --- Processor Models/ API Start ---

case class ProcessorConfig(@BeanProperty var bulletinLevel: String,
                           @BeanProperty var comments: String,
                           @BeanProperty var concurrentlySchedulableTaskCount: Int,
                           @BeanProperty var penaltyDuration: String,
                           @BeanProperty var schedulingPeriod: String,
                           @BeanProperty var schedulingStrategy: String,
                           @BeanProperty var yieldDuration: String) {
  def this() = this("", "", 1, "", "", "", "")
}

case class ProcessorInstance(@BeanProperty var id: String,
                             @BeanProperty var name: String,
                             @BeanProperty var `type`: String,
                             @BeanProperty var processorType: String,
                             @BeanProperty var status: String,
                             @BeanProperty var version: Long,
                             @BeanProperty var properties: Map[String, String],
                             @BeanProperty var propertyDefinitions: List[RemoteProperty],
                             @BeanProperty var validationErrors: List[String],
                             @BeanProperty var config: ProcessorConfig) {
  def this() = this("", "", "", "", "", 0.0.toLong, Map(), Nil, Nil, new ProcessorConfig())
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

case class ProcessorDetails(@BeanProperty var metadata: MetaData,
                            @BeanProperty var configuration: Configuration,
                            @BeanProperty var relationships: util.Set[RemoteRelationship])

trait ProcessorApiService {
  def types(): Future[List[ProcessorType]]
  def typesSearchTags(str:String): Future[List[ProcessorType]]
  def create(processorServiceDefinition: ProcessorServiceDefinition,
             processGroupId: String,
             clientId: String): Future[ProcessorInstance]
  def update(processorInstance: ProcessorInstance, clientId: String): Future[ProcessorInstance]
  def updateProperties(processorId: String, properties: Map[String, String], clientId : String): Future[ProcessorInstance]
  def updateSchema(flowInstanceId: String,
                   processorInstanceId: String,
                   schemaActions: List[SchemaAction],
                   clientId: String): Future[List[ProcessorInstance]]
  def instance(processorId: String): Future[ProcessorInstance]
  def start(processorId: String, version: Long, clientId: String): Future[ProcessorInstance]
  def stop(processorId: String, version: Long, clientId: String): Future[ProcessorInstance]
  def remove(processorId: String, version: Long, clientId: String): Future[Boolean]
}



// --- Processor Models/ API End ---

// --- Connection Models/ API Start ---

case class Connectable(@BeanProperty var id: String,
                       @BeanProperty var componentType: String,
                       @BeanProperty var flowInstanceId: String) {
  def this() = this("", "", "")
}

case class ConnectionCreate(@BeanProperty var flowInstanceId: String,
                            @BeanProperty var source: Connectable,
                            @BeanProperty var destination: Connectable,
                            @BeanProperty var sourceRelationships: Set[String]) {
  def this() = this("", Connectable("", "", ""), Connectable("", "", ""), Set())
}

case class Connection(@BeanProperty var id: String,
                      @BeanProperty var flowInstanceId: String,
                      @BeanProperty var name: String,
                      @BeanProperty var version: Long,
                      @BeanProperty var source: Connectable,
                      @BeanProperty var destination: Connectable,
                      @BeanProperty var sourceRelationships: Set[String],
                      @BeanProperty var flowFileExpiration: String,
                      @BeanProperty var backPressureDataSize: String,
                      @BeanProperty var backPressureObjectThreshold: Long,
                      @BeanProperty var prioritizers: List[String]) {
  def this() = this("", "", "", 0, Connectable("", "", ""), Connectable("", "", ""), Set(), "", "", -1, Nil)
}

trait ConnectionApiService {
  def create(connectionCreate: ConnectionCreate, clientId: String): Future[Connection]
  def update(connection: Connection, clientId: String): Future[Connection]
  def remove(connectionId: String, version: Long, clientId: String): Future[Boolean]
}

// --- Connection Models/ API End ---


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

object FlowComponent {
  val ProcessorType = "PROCESSOR"
  val RemoteInputPortType = "REMOTE_INPUT_PORT"
  val RemoteOutputPortType = "REMOTE_OUTPUT_PORT"
  val InputPortType = "INPUT_PORT"
  val OutputPortType = "OUTPUT_PORT"
  val FunnelType = "FUNNEL"
}