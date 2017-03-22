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

    val workerProcessor = new TestWorkerProcessor()
    val workerProcessorWithSchema = new TestWorkerProcessorWithSchema()

    val readSchema = AvroSchemaStore.get(sid)
    val writeSchema = AvroSchemaStore.get(swoageid)

    val input = person.serToBytes(readSchema)

    val readSchemaJson =
      using(this.getClass.getResourceAsStream("/avro/" + sid + ".avsc")) { is =>
        IOUtils.toString(is)
      }

    val writeSchemaJson =
      using(this.getClass.getResourceAsStream("/avro/" + swoageid + ".avsc")) { is =>
        IOUtils.toString(is)
      }

    "resolve with the provided read schema / default write schema" in {
      assertResult(TestWorkerProcessor.personWoAge) {
        workerProcessorWithSchema.
          trigger(input,
            Map(ReadSchemaKey -> readSchemaJson).asJava)(1).
          deSerToGenericRecord(writeSchema, writeSchema)
      }
    }

    "resolve with the provided read schema" in {
      assertResult(TestWorkerProcessor.person) {
        workerProcessor.
          trigger(input,
            Map(ReadSchemaKey -> readSchemaJson).asJava)(1).
          deSerToGenericRecord(readSchema, readSchema)
      }
    }

    "resolve with the provided read schema id" in {
      assertResult(TestWorkerProcessor.person) {
        workerProcessor.
          trigger(input,
            Map(ReadSchemaIdKey -> sid).asJava)(1).
          deSerToGenericRecord(readSchema, readSchema)
      }
    }

    "resolve with the provided read schema id / write schema id" in {
      assertResult(TestWorkerProcessor.personWoAge) {
        workerProcessor.
          trigger(input,
            Map(ReadSchemaIdKey -> sid, WriteSchemaIdKey -> swoageid).asJava)(1).
          deSerToGenericRecord(writeSchema, writeSchema)
      }
    }

    "resolve with the provided read schema / write schema id" in {
      assertResult(TestWorkerProcessor.personWoAge) {
        workerProcessor.
          trigger(input,
            Map(ReadSchemaKey -> readSchemaJson, WriteSchemaIdKey -> swoageid).asJava)(1).
          deSerToGenericRecord(writeSchema, writeSchema)
      }
    }

    "resolve with the provided read schema id / write schema" in {
      assertResult(TestWorkerProcessor.personWoAge) {
        workerProcessor.
          trigger(input,
            Map(ReadSchemaIdKey -> sid, WriteSchemaKey -> writeSchemaJson).asJava)(1).
          deSerToGenericRecord(writeSchema, writeSchema)
      }
    }

    "throw an exception if write schema is provided but no read schema " in {
          assertResult(true) {
            val error = workerProcessor.
              trigger(input, Map(WriteSchemaIdKey -> sid).asJava)(1).
              deSerToGenericRecord(Some(AvroSchemaStore.errorResponseSchema()),
                Some(AvroSchemaStore.errorResponseSchema()))
            (error.get("code").toString == "DCS306") &&
              (error.get("errorMessage").toString == "Read Schema for  " +
                workerProcessor.className + " not available")
          }
    }

    "throw an exception if no read or write schema is provided" in {
      assertResult(true) {
        val error = workerProcessor.
          trigger(input, Map[String, String]().asJava)(1).
          deSerToGenericRecord(Some(AvroSchemaStore.errorResponseSchema()),
            Some(AvroSchemaStore.errorResponseSchema()))
        (error.get("code").toString == "DCS306") &&
          (error.get("errorMessage").toString == "Read Schema for  " +
            workerProcessor.className + " not available")
      }
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

  val personWoAge = new GenericData.Record(AvroSchemaStore.get(swoageid).get)
  personWoAge.put("first_name", FirstName)
  personWoAge.put("middle_name", MiddleName)
  personWoAge.put("last_name", LastName)
}

class TestWorkerProcessor extends Worker {
  import TestWorkerProcessor._

  override def execute(record: Option[GenericRecord],
                       properties: util.Map[String, String]): List[Either[ErrorResponse, AnyRef]] = {

    List(Right(person))
  }

  override def metadata(): MetaData = MetaData()

  override def schemaId: String = null
}

class TestWorkerProcessorWithSchema extends TestWorkerProcessor {
  import TestWorkerProcessor._

  override def schemaId: String = swoageid
}




