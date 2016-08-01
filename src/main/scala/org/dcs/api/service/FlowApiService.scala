package org.dcs.api.service

import java.util.Date

import scala.beans.BeanProperty

/**
  * Created by cmathew on 05/06/16.
  */

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
                        @BeanProperty var version: String,
                        @BeanProperty var processors : List[ProcessorInstance],
                        @BeanProperty var connections: List[Connection]) {
  def this() = this("", "", Nil, Nil)
}

case class FlowTemplate(@BeanProperty var id: String,
                        @BeanProperty var uri: String,
                        @BeanProperty var name: String,
                        @BeanProperty var description: String,
                        @BeanProperty var timestamp: Date) {
  def this() = this("", "", "", "", null)
}

case class ProcessorInstance(@BeanProperty var id: String,
                             @BeanProperty var status: String) {
  def this() = this("", "")
}

case class ProcessorType(@BeanProperty var pType:String,
                         @BeanProperty var description:String,
                         @BeanProperty var tags: List[String]) {
  def this() = this("", "", Nil)
}

trait FlowApiService {
  def templates(clientId: String):List[FlowTemplate]
  def instantiate(flowTemplateId:String, clientId: String):FlowInstance
  def instance(flowInstanceId: String, clientId: String): FlowInstance
  def remove(flowInstanceId: String, clientId: String): Boolean
}

trait ProcessorApiService {
  def types(clientToken: String): List[ProcessorType]
  def typesSearchTags(str:String, clientToken: String): List[ProcessorType]
  def create(name: String, ptype: String, clientToken: String): ProcessorInstance
  def start(processorId: String, clientToken: String): ProcessorInstance
  def remove(processorId: String, clientToken: String): Boolean
}

