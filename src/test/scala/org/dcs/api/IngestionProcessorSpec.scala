package org.dcs.api

import java.util.{Map => JavaMap}

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.apache.commons.io.IOUtils
import org.dcs.api.processor.{CoreProperties, Ingestion, MetaData, RelationshipType}
import org.dcs.commons.error.ErrorResponse
import org.dcs.commons.serde.AvroSchemaStore

import scala.collection.JavaConverters._
import org.dcs.commons.Control._
import org.dcs.commons.serde.AvroImplicits._

/**
  * Created by cmathew on 16.03.17.
  */

class IngestionProcessorSpec extends ApiUnitWordSpec {
  import TestIngestionProcessor._


  "Ingestion Processor Schema Read / Write" should  {

    val writeSchema: Option[Schema] = AvroSchemaStore.get(sid)

    "resolve with the default schema" in {
      assertResult(TestIngestionProcessor.person) {
        new TestIngestionProcessor().
          trigger(Array.emptyByteArray, Map[String, String]().asJava)(1).
          deSerToGenericRecord(writeSchema, writeSchema)
      }
    }


    "resolve with the provided with empty write schema" in {
      assertResult(TestIngestionProcessor.person) {
        new TestIngestionProcessor().
          trigger(Array.emptyByteArray, Map(CoreProperties.WriteSchemaKey -> "").asJava)(1).
          deSerToGenericRecord(writeSchema, writeSchema)
      }
    }

    val writeSchemaJson =
      using(this.getClass.getResourceAsStream("/avro/" + sid + ".avsc")) { is =>
        IOUtils.toString(is)
      }

    "resolve with the provided write schema" in {
      assertResult(TestIngestionProcessor.person) {
        new TestIngestionProcessorWOSchemaId().
          trigger(Array.emptyByteArray, Map(CoreProperties.WriteSchemaKey -> writeSchemaJson).asJava)(1).
          deSerToGenericRecord(writeSchema, writeSchema)
      }
    }


    "resolve with the provided write schema id" in {
      assertResult(TestIngestionProcessor.person) {
        new TestIngestionProcessorWOSchemaId().
          trigger(Array.emptyByteArray,
            Map(CoreProperties.WriteSchemaIdKey -> TestIngestionProcessor.sid).asJava)(1).
          deSerToGenericRecord(writeSchema, writeSchema)
      }
    }

    "throw an exception when no default schema exists and no schema is provided" in {

        val errorResponse = new TestIngestionProcessorWOSchemaId().
          trigger(Array.emptyByteArray, Map[String, String]().asJava)(1).
          deSerToGenericRecord(Some(AvroSchemaStore.errorResponseSchema()),
            Some(AvroSchemaStore.errorResponseSchema()))
        println("errorResponse : " + errorResponse.toString)
        assert(errorResponse.get("code").toString == "DCS306")

        assert(errorResponse.get("errorMessage").toString.startsWith("Write Schema for"))

    }
  }
}

object TestIngestionProcessor {

  AvroSchemaStore.add("org.dcs.api.TestIngestionProcessor", this.getClass.getResourceAsStream("/avro/" + "org.dcs.api.TestIngestionProcessor" + ".avsc"))

  val sid = "org.dcs.api.Person"
  AvroSchemaStore.add(sid, this.getClass.getResourceAsStream("/avro/" + sid + ".avsc"))


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
}

class TestIngestionProcessor extends Ingestion {
  import TestIngestionProcessor._

  override def execute(record: Option[GenericRecord],
                       properties: JavaMap[String, String]): List[Either[ErrorResponse, (String, AnyRef)]] = {

    List(Right((RelationshipType.Success.id, person)))
  }

  override def metadata(): MetaData = MetaData("")

}

class TestIngestionProcessorWOSchemaId extends TestIngestionProcessor {
  override def schemaId: String = null
}


