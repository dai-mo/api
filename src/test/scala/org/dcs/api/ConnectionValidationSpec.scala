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

package org.dcs.api

import org.dcs.api.processor.{ConnectionValidation, CoreProperties, RemoteProcessor}
import org.dcs.api.service.{Connectable, ConnectionConfig, FlowComponent}
import org.dcs.commons.error.{DCSException, ErrorConstants}

class ConnectionValidationSpec extends ApiUnitWordSpec {

  "Connection Validation" should {

    "run without error when trying to connect ingestion -> sink / ingestion -> worker / worker -> sink processor" in {
      val ingestionSinkConfig =
        ConnectionConfig("",
          Connectable("", FlowComponent.ProcessorType, "", Map(CoreProperties.ProcessorTypeKey -> RemoteProcessor.IngestionProcessorType)),
          Connectable("", FlowComponent.ProcessorType, "", Map(CoreProperties.ProcessorTypeKey -> RemoteProcessor.SinkProcessorType)),
          Set("success"),
          Set("success"))
      ConnectionValidation.validate(ingestionSinkConfig)

      val ingestionWorkerConfig =
        ConnectionConfig("",
          Connectable("", FlowComponent.ProcessorType, "", Map(CoreProperties.ProcessorTypeKey -> RemoteProcessor.IngestionProcessorType)),
          Connectable("", FlowComponent.ProcessorType, "", Map(CoreProperties.ProcessorTypeKey -> RemoteProcessor.WorkerProcessorType)),
          Set("success"),
          Set("success"))
      ConnectionValidation.validate(ingestionWorkerConfig)

      val sinkWorkerConfig =
        ConnectionConfig("",
          Connectable("", FlowComponent.ProcessorType, "", Map(CoreProperties.ProcessorTypeKey -> RemoteProcessor.WorkerProcessorType)),
          Connectable("", FlowComponent.ProcessorType, "", Map(CoreProperties.ProcessorTypeKey -> RemoteProcessor.SinkProcessorType)),
          Set("success"),
          Set("success"))
      ConnectionValidation.validate(sinkWorkerConfig)

    }

    "return error when trying to connect a worker -> ingestion processor" in {
      val workerIngestionConfig =
        ConnectionConfig("",
          Connectable("", FlowComponent.ProcessorType, "", Map(CoreProperties.ProcessorTypeKey -> RemoteProcessor.WorkerProcessorType)),
          Connectable("", FlowComponent.ProcessorType, "", Map(CoreProperties.ProcessorTypeKey -> RemoteProcessor.IngestionProcessorType)),
          Set("success"),
          Set("success"))
      val caught =
        intercept[DCSException] {
          ConnectionValidation.validate(workerIngestionConfig)
        }
      assert(caught.errorResponse.code == ErrorConstants.DCS315.code)
    }

    "return error when trying to connect a sink -> ingestion / sink -> worker processor" in {
      val sinkIngestionConfig =
        ConnectionConfig("",
          Connectable("", FlowComponent.ProcessorType, "", Map(CoreProperties.ProcessorTypeKey -> RemoteProcessor.SinkProcessorType)),
          Connectable("", FlowComponent.ProcessorType, "", Map(CoreProperties.ProcessorTypeKey -> RemoteProcessor.IngestionProcessorType)),
          Set("success"),
          Set("success"))
      var caught =
        intercept[DCSException] {
          ConnectionValidation.validate(sinkIngestionConfig)
        }
      assert(caught.errorResponse.code == ErrorConstants.DCS315.code)

      val sinkWorkerConfig =
        ConnectionConfig("",
          Connectable("", FlowComponent.ProcessorType, "", Map(CoreProperties.ProcessorTypeKey -> RemoteProcessor.SinkProcessorType)),
          Connectable("", FlowComponent.ProcessorType, "", Map(CoreProperties.ProcessorTypeKey -> RemoteProcessor.WorkerProcessorType)),
          Set("success"),
          Set("success"))
      caught =
        intercept[DCSException] {
          ConnectionValidation.validate(sinkWorkerConfig)
        }
      assert(caught.errorResponse.code == ErrorConstants.DCS315.code)
    }
  }

}
