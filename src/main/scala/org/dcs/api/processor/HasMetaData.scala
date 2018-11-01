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

package org.dcs.api.processor

import java.util.{List => JavaList}

import scala.beans.BeanProperty
import scala.collection.JavaConverters._

/**
  * Created by cmathew on 06/09/16.
  */
trait HasMetaData {

  def metadata():MetaData
}

object MetaData {
  def apply(description: String, tags: List[String] = List(), related: List[String] = List()): MetaData =
    new MetaData(description, tags.asJava, related.asJava)
}
case class MetaData(@BeanProperty var description: String ,
                    @BeanProperty var tags: JavaList[String],
                    @BeanProperty var related: JavaList[String]) {
  def this() = this("", List().asJava, List().asJava)
}