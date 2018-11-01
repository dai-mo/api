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

package org.dcs.api.service

import java.util
import java.util.Date

import org.dcs.api.processor._
import org.dcs.commons.SchemaAction
import org.dcs.commons.error.ValidationErrorResponse

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

  def externalConnections: List[Connection] = {
    connections.filter(_.config.isExternal())
  }

  def externalProcessors: List[ProcessorInstance] = {
    processors.filter(_.isExternal)
  }

  def hasExternalConnection: Boolean =
    connections.exists(_.config.isExternal())

  def hasExternalProcessors: Boolean =
    processors.exists(_.isExternal)

  def rootPortIdVersions: Map[String, String] = connections.flatMap(_.rootPortIdVersions()).toMap
}

case class FlowTemplate(@BeanProperty var id: String,
                        @BeanProperty var uri: String,
                        @BeanProperty var name: String,
                        @BeanProperty var description: String,
                        @BeanProperty var timestamp: Date,
                        @BeanProperty var hasExternal: Boolean = false) {
  def this() = this("", "", "", "", null, false)
}



trait FlowApiService {
  def templates(): Future[List[FlowTemplate]]
  def create(flowName: String, clientId: String): Future[FlowInstance]
  def instantiate(flowTemplateId: String, clientId: String): Future[FlowInstance]
  def instance(flowInstanceId: String, flowInstanceName: String, externalConnections: List[Connection], clientId: String): Future[FlowInstance]
  def instance(flowInstanceId: String, clientId: String): Future[FlowInstance]
  // def instance(flowInstanceId: String): Future[FlowInstance]
  def instances(): Future[List[FlowInstance]]
  def updateName(name: String, flowInstanceId: String, version: Long, clientId: String): Future[FlowInstance]
  def start(flowInstanceId: String, clientId: String): Future[FlowInstance]
  def stop(flowInstanceId: String, clientId: String): Future[FlowInstance]
  def remove(flowInstanceId: String, version: Long, clientId: String, hasExternal: Boolean = false): Future[Boolean]
  def remove(flowInstanceId: String, version: Long, clientId: String, externalConnections: List[Connection]): Future[Boolean]
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

object ProcessorInstance {
  def isExternal(processorType: String): Boolean =
    processorType == RemoteProcessor.ExternalProcessorType || processorType == RemoteProcessor.InputPortIngestionType
}

case class ProcessorInstance(@BeanProperty var id: String,
                             @BeanProperty var name: String,
                             @BeanProperty var `type`: String,
                             @BeanProperty var processorType: String,
                             @BeanProperty var status: String,
                             @BeanProperty var version: Long,
                             @BeanProperty var properties: Map[String, String],
                             @BeanProperty var propertyDefinitions: List[RemoteProperty],
                             @BeanProperty var relationships: Set[RemoteRelationship],
                             @BeanProperty var validationErrors: ValidationErrorResponse,
                             @BeanProperty var config: ProcessorConfig) {
  def this() = this("", "", "", "", "", 0.0.toLong, Map(), Nil, Set(), null, new ProcessorConfig())

  def isExternal: Boolean =
    ProcessorInstance.isExternal(processorType)
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
                            @BeanProperty var relationships: util.Set[RemoteRelationship]) {
  def this() = this(new MetaData(), new Configuration(), new util.HashSet[RemoteRelationship]())
}

trait ProcessorApiService {
  def types(): Future[List[ProcessorType]]
  def typesSearchTags(str:String): Future[List[ProcessorType]]
  def create(processorServiceDefinition: ProcessorServiceDefinition,
             processGroupId: String,
             clientId: String): Future[ProcessorInstance]
  def autoTerminateRelationship(connection: Connection): Future[ProcessorInstance]
  def update(processorInstance: ProcessorInstance, clientId: String): Future[ProcessorInstance]
  def updateProperties(processorId: String, properties: Map[String, String], clientId : String): Future[ProcessorInstance]
  def updateSchemaProperty(processorId: String, schemaPropertyKey: String, schema: String, flowInstanceId: String, clientId : String): Future[ProcessorInstance]
  def updateSchema(flowInstanceId: String,
                   processorInstanceId: String,
                   schemaActions: List[SchemaAction],
                   clientId: String): Future[List[ProcessorInstance]]
  def instance(processorId: String, validate: Boolean = true): Future[ProcessorInstance]
  def start(processorId: String, version: Long, clientId: String): Future[ProcessorInstance]
  def stop(processorId: String, version: Long, clientId: String): Future[ProcessorInstance]
  def remove(processorId: String, version: Long, clientId: String): Future[Boolean]
  def remove(processorId: String,
             flowInstanceId: String,
             processorType: String,
             version: Long,
             clientId: String): Future[Boolean]
}



// --- Processor Models/ API End ---

// --- Connection Models/ API Start ---

case class Connectable(@BeanProperty var id: String,
                       @BeanProperty var componentType: String,
                       @BeanProperty var flowInstanceId: String,
                       @BeanProperty var properties: Map[String, String] = Map(),
                       @BeanProperty var name: String = "") {
  def this() = this("", "", "", Map(), "")
}

object ConnectionConfig {
  def inputPortIngestionConnection(flowInstanceId: String, processorId: String, processorName: String) =
    new ConnectionConfig(flowInstanceId,
      Connectable("", "", ""),
      Connectable(processorId, FlowComponent.InputPortIngestionType, flowInstanceId, name = processorName))
}

case class ConnectionConfig(@BeanProperty var flowInstanceId: String,
                            @BeanProperty var source: Connectable,
                            @BeanProperty var destination: Connectable,
                            @BeanProperty var selectedRelationships: Set[String] = Set(),
                            @BeanProperty var availableRelationships: Set[String] = Set()) {
  def this() = this("", Connectable("", "", ""), Connectable("", "", ""))
  def genId(): String = source.id + "-" + destination.id
  def isExternal(): Boolean =
    if(source.componentType == FlowComponent.ExternalProcessorType ||
      destination.componentType == FlowComponent.ExternalProcessorType ||
      source.componentType == FlowComponent.InputPortIngestionType ||
      destination.componentType == FlowComponent.InputPortIngestionType)
      true
    else
      false
}


case class Connection(@BeanProperty var id: String,
                      @BeanProperty var name: String,
                      @BeanProperty var version: Long,
                      @BeanProperty var config: ConnectionConfig,
                      @BeanProperty var flowFileExpiration: String,
                      @BeanProperty var backPressureDataSize: String,
                      @BeanProperty var backPressureObjectThreshold: Long,
                      @BeanProperty var prioritizers: List[String],
                      @BeanProperty var relatedConnections: Set[Connection] = Set()) {
  //  import Connection._

  def this() = this("", "", 0, new ConnectionConfig(), "", "", -1, Nil)

  def withConnection(connection: Connection): Connection = {
    relatedConnections = relatedConnections + connection
    this
  }

  def rootPortIdVersions(): Map[String, String] = {
    val currentRootPortId: Option[(String, String)] =
      if(config.source.componentType == FlowComponent.InputPortType &&
        config.destination.componentType == FlowComponent.InputPortType) {
        Some(config.source.id -> config.source.componentType)
      } else if (config.source.componentType == FlowComponent.OutputPortType &&
        config.destination.componentType == FlowComponent.OutputPortType) {
        Some(config.destination.id -> config.destination.componentType)
      } else
        None

    if(currentRootPortId.isDefined)
      relatedConnections.flatMap(_.rootPortIdVersions()).toMap + currentRootPortId.get
    else
      relatedConnections.flatMap(_.rootPortIdVersions()).toMap

  }

}

trait ConnectionApiService {
  def list(processGroupId: String): Future[List[Connection]]
  def find(connectionId: String, clientId: String): Future[Connection]
  def create(connectionConfig: ConnectionConfig, clientId: String): Future[Connection]
  def createProcessorConnection(connectionConfig: ConnectionConfig, clientId: String): Future[Connection]
  def createStdConnection(connectionConfig: ConnectionConfig, clientId: String): Future[Connection]
  def update(connection: Connection, clientId: String): Future[Connection]
  def remove(connectionId: String, version: Long, clientId: String): Future[Boolean]
  def remove(connection: Connection, clientId: String): Future[Boolean]
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

case class IOPort(@BeanProperty id: String,
                  @BeanProperty name: String,
                  @BeanProperty version: Long,
                  @BeanProperty `type`: String,
                  @BeanProperty status: String) {
  def this() = this("", "", 0L, "", "")
}

trait IFlowDataService {
  def provenanceByComponentId(cid: String, maxResults: Int): util.List[Provenance]
}

// --- Flow Data Models / API end ---

// --- Flow IO Port Models / API start ---

trait IOPortApiService {
  def inputPort(id: String): Future[IOPort]
  def outputPort(id: String): Future[IOPort]
  def createInputPort(processGroupId: String, clientId: String): Future[Connection]
  def createOutputPort(processGroupId: String, clientId: String): Future[Connection]
  def updateInputPortName(portName: String, portId: String, clientId: String): Future[IOPort]
  def updateOutputPortName(portName: String, portId: String, clientId: String): Future[IOPort]
  def deleteInputPort(inputPortId: String, clientId: String): Future[Option[IOPort]]
  def deleteInputPort(rootPortId: String, inputPortId: String, clientId: String): Future[Boolean]
  def deleteOutputPort(outputPortId: String, clientId: String):  Future[Option[IOPort]]
  def deleteOutputPort(outputPortId: String, rootPortId: String, clientId: String): Future[Boolean]
}

// --- Flow IO Port Models / API end ---

// --- Flow Drop Request Models / API start ---

case class DropRequest(@BeanProperty var id: String,
                       @BeanProperty var finished: Boolean,
                       @BeanProperty var currentCount: Int) {
  def this() = this("", true, 0)
}

// --- Flow Drop Request Models / API end ---

object FlowComponent {
  val ProcessorType = "PROCESSOR"
  val ExternalProcessorType = "EXTERNAL_PROCESSOR"
  val InputPortIngestionType = "INPUT_PORT_INGESTION_TYPE"
  val RemoteInputPortType = "REMOTE_INPUT_PORT"
  val RemoteOutputPortType = "REMOTE_OUTPUT_PORT"
  val InputPortType = "INPUT_PORT"
  val OutputPortType = "OUTPUT_PORT"
  val FunnelType = "FUNNEL"

}