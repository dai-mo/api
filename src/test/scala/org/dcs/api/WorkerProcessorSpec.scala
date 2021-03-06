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

import java.util.{Map => JavaMap}

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.commons.io.IOUtils
import org.dcs.api.processor.CoreProperties._
import org.dcs.api.processor._
import org.dcs.commons.Control._
import org.dcs.commons.error.ErrorResponse
import org.dcs.commons.serde.AvroImplicits._
import org.dcs.commons.serde.AvroSchemaStore

import scala.collection.JavaConverters._

/**
  * Created by cmathew on 16.03.17.
  */

class WorkerProcessorSpec extends ApiUnitWordSpec {
  import TestWorkerProcessor._

  "Worker Processor Schema Read / Write" should  {

    val personSchema = AvroSchemaStore.get(sid)
    val personWOAgeSchema = AvroSchemaStore.get(swoageid)

    val input = person.serToBytes(personSchema)

    val personSchemaJson =
      using(this.getClass.getResourceAsStream("/avro/" + sid + ".avsc")) { is =>
        IOUtils.toString(is)
      }

    val personWOAgeSchemaJson =
      using(this.getClass.getResourceAsStream("/avro/" + swoageid + ".avsc")) { is =>
        IOUtils.toString(is)
      }

    "resolve with the provided read schema / default write schema" in {
      assertResult(TestWorkerProcessor.personWoAge) {
        new TestWorkerProcessorWithSchema().
          trigger(input,
            Map(ReadSchemaKey -> personSchemaJson).asJava)(1).
          deSerToGenericRecord(personWOAgeSchema, personWOAgeSchema)
      }
    }

    "resolve with the provided read schema" in {
      assertResult(TestWorkerProcessor.person) {
        new TestWorkerProcessor().
          trigger(input,
            Map(ReadSchemaKey -> personSchemaJson).asJava)(1).
          deSerToGenericRecord(personSchema, personSchema)
      }
    }

    "resolve with the provided read schema for input with nulls" in {
      assertResult(TestWorkerProcessor.personWithNulls) {
        new TestWorkerProcessor().
          trigger(TestWorkerProcessor.personWithNulls.serToBytes(personSchema),
            Map(ReadSchemaKey -> personSchemaJson).asJava)(1).
          deSerToGenericRecord(personSchema, personSchema)
      }
    }

    "resolve with the provided read schema without field" in {
      assertResult(TestWorkerProcessor.personWoAge) {
        new TestWorkerProcessor().
          trigger(input,
            Map(ReadSchemaIdKey -> sid, WriteSchemaKey -> personWOAgeSchemaJson).asJava)(1).
          deSerToGenericRecord(personWOAgeSchema, personWOAgeSchema)
      }
    }

    "resolve with the provided read schema id" in {
      assertResult(TestWorkerProcessor.person) {
        new TestWorkerProcessor().
          trigger(input,
            Map(ReadSchemaIdKey -> sid).asJava)(1).
          deSerToGenericRecord(personSchema, personSchema)
      }
    }

    "resolve with the provided read schema id / write schema id" in {
      assertResult(TestWorkerProcessor.personWoAge) {
        new TestWorkerProcessor().
          trigger(input,
            Map(ReadSchemaIdKey -> sid, WriteSchemaIdKey -> swoageid).asJava)(1).
          deSerToGenericRecord(personWOAgeSchema, personWOAgeSchema)
      }
    }

    "resolve with the provided read schema / write schema id" in {
      assertResult(TestWorkerProcessor.personWoAge) {
        new TestWorkerProcessor().
          trigger(input,
            Map(ReadSchemaKey -> personSchemaJson, WriteSchemaIdKey -> swoageid).asJava)(1).
          deSerToGenericRecord(personWOAgeSchema, personWOAgeSchema)
      }
    }

    "resolve with the provided read schema id / write schema" in {
      assertResult(TestWorkerProcessor.personWoAge) {
        new TestWorkerProcessor().
          trigger(input,
            Map(ReadSchemaIdKey -> sid, WriteSchemaKey -> personWOAgeSchemaJson).asJava)(1).
          deSerToGenericRecord(personWOAgeSchema, personWOAgeSchema)
      }
    }

    var er1 = new TestWorkerProcessor().
      trigger(input, Map(WriteSchemaIdKey -> sid).asJava)(1).
      deSerToGenericRecord(Some(AvroSchemaStore.errorResponseSchema()),
        Some(AvroSchemaStore.errorResponseSchema()))


    "throw an exception if write schema is provided but no read schema " in {
      assert(er1.get("code").toString == "DCS306" &&
        er1.get("errorMessage").toString.startsWith("Read Schema for"))
    }

    val er2 = new TestWorkerProcessor().
      trigger(input, Map[String, String]().asJava)(1).
      deSerToGenericRecord(Some(AvroSchemaStore.errorResponseSchema()),
        Some(AvroSchemaStore.errorResponseSchema()))

    "throw an exception if no read or write schema is provided" in {
      assert(er2.get("code").toString == "DCS306" &&
        er2.get("errorMessage").toString.startsWith("Read Schema for"))
    }
  }
}
object TestWorkerProcessor {


  val sid = "org.dcs.api.Person"
  AvroSchemaStore.add(sid, this.getClass.getResourceAsStream("/avro/" + sid + ".avsc"))

  val swoageid = "org.dcs.api.PersonWOAge"
  AvroSchemaStore.add(swoageid, this.getClass.getResourceAsStream("/avro/" + swoageid + ".avsc"))


  val FirstNameKey = "first_name"
  val FirstName = "Obi"

  val MiddleNameKey = "middle_name"
  val MiddleName = "Wan"

  val LastNameKey = "last_name"
  val LastName = "Kenobi"

  val AgeKey = "age"
  val Age = 9999


  val person = new GenericData.Record(AvroSchemaStore.get(sid).get)
  person.put("first_name", FirstName)
  person.put("middle_name", MiddleName)
  person.put("last_name", LastName)
  person.put("age", Age)

  val personWithNulls = new GenericData.Record(AvroSchemaStore.get(sid).get)
  personWithNulls.put("first_name", null)
  personWithNulls.put("middle_name", MiddleName)
  personWithNulls.put("last_name", null)
  personWithNulls.put("age", Age)

  val personWoAge = new GenericData.Record(AvroSchemaStore.get(swoageid).get)
  personWoAge.put("first_name", FirstName)
  personWoAge.put("middle_name", MiddleName)
  personWoAge.put("last_name", LastName)
}

class TestWorkerProcessor extends Worker {

  override def execute(record: Option[GenericRecord],
                       properties: JavaMap[String, String]): List[Either[ErrorResponse, (String, AnyRef)]] = {

    List(Right((RelationshipType.Success.id, record.get)))
  }

  override def metadata(): MetaData = MetaData("")

  override def schemaId: String = null
}

class TestWorkerProcessorWithSchema extends TestWorkerProcessor {
  import TestWorkerProcessor._

  override def execute(record: Option[GenericRecord],
                       properties: JavaMap[String, String]): List[Either[ErrorResponse, (String, AnyRef)]] = {

    List(Right((RelationshipType.Success.id, personWoAge)))
  }

  override def schemaId: String = swoageid
}




