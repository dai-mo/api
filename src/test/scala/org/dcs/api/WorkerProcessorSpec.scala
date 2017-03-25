package org.dcs.api

import java.util

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.apache.commons.io.IOUtils
import org.dcs.api.processor.{CoreProperties, Ingestion, MetaData, Worker}
import org.dcs.commons.error.ErrorResponse
import org.dcs.commons.serde.AvroSchemaStore

import scala.collection.JavaConverters._
import org.dcs.commons.Control._
import org.dcs.commons.serde.AvroImplicits._
import CoreProperties._

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

    "throw an exception if write schema is provided but no read schema " in {
      val error = new TestWorkerProcessor().
        trigger(input, Map(WriteSchemaIdKey -> sid).asJava)(1).
        deSerToGenericRecord(Some(AvroSchemaStore.errorResponseSchema()),
          Some(AvroSchemaStore.errorResponseSchema()))
      assert(error.get("code").toString == "DCS306")
      assert(error.get("errorMessage").toString.startsWith("Read Schema for"))
    }

    "throw an exception if no read or write schema is provided" in {
      val error = new TestWorkerProcessor().
        trigger(input, Map[String, String]().asJava)(1).
        deSerToGenericRecord(Some(AvroSchemaStore.errorResponseSchema()),
          Some(AvroSchemaStore.errorResponseSchema()))
      assert(error.get("code").toString == "DCS306")
      assert(error.get("errorMessage").toString.startsWith("Read Schema for"))
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
                       properties: util.Map[String, String]): List[Either[ErrorResponse, AnyRef]] = {

    List(Right(record.get))
  }

  override def metadata(): MetaData = MetaData()

  override def schemaId: String = null
}

class TestWorkerProcessorWithSchema extends TestWorkerProcessor {
  import TestWorkerProcessor._

  override def execute(record: Option[GenericRecord],
                       properties: util.Map[String, String]): List[Either[ErrorResponse, AnyRef]] = {

    List(Right(personWoAge))
  }

  override def schemaId: String = swoageid
}




