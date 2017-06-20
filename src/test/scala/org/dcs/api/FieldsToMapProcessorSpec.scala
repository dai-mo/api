package org.dcs.api

import java.util

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.dcs.api.processor.{FieldsToMap, _}
import org.dcs.commons.error.ErrorResponse
import org.dcs.commons.serde.AvroSchemaStore

import scala.collection.JavaConverters._
import org.dcs.commons.serde.JsonSerializerImplicits._

/**
  * Created by cmathew on 22.03.17.
  */
class FieldsToMapProcessorSpec extends ApiUnitWordSpec {
  import TestFieldsToMapProcessor._


  "Fields To Map Processor" should {
    val fieldsToMapProcessor = new TestFieldsToMapProcessor

    val defaultFieldsToMapPropertyValue = Map(FirstNameKey -> "",
      MiddleNameKey -> "",
      LastNameKey -> "").toJson

    val fieldsToMapPropertyValue = Map(FirstNameKey -> ("$.name." + FirstNameSchemaKey),
      MiddleNameKey -> ("$.name." + MiddleNameSchemaKey),
      LastNameKey -> ("$.name." + LastNameSchemaKey)).toJson

    "validate fields to map with schema" in {
      assertThrows[IllegalStateException] {
        FieldsToMap.schemaCheck(schema.get, defaultFieldsToMapPropertyValue)
      }

      assertThrows[IllegalStateException] {
        FieldsToMap.schemaCheck(schema.get, defaultFieldsToMapPropertyValue + (MiddleNameKey -> ("$.name." + MiddleNameSchemaKey)))
      }

      assert(FieldsToMap.schemaCheck(schema.get, fieldsToMapPropertyValue))
    }


    "return correct default value for fields to map property" in {
      println("exp : " + defaultFieldsToMapPropertyValue)
      println("act : " + fieldsToMapProcessor.properties().asScala.find(p => p.name == CoreProperties.FieldsToMapKey).get.defaultValue)
      assertResult(defaultFieldsToMapPropertyValue) {
        fieldsToMapProcessor.properties().asScala.find(p => p.name == CoreProperties.FieldsToMapKey).get.defaultValue
      }
    }

    "return correct field values for nulls" in {
      val m = fieldsToMapProcessor.mappings(Some(personWithNulls),
        Map(CoreProperties.FieldsToMapKey -> fieldsToMapPropertyValue))

      val fname = m.get(FirstNameKey)
      assert(fname.isEmpty)

      val lname = m.get(LastNameKey)
      assert(lname.isEmpty)
    }

    "provide valid field mappings for correct json path <-> record combinations" in {
      val m = fieldsToMapProcessor.mappings(Some(person),
        Map(CoreProperties.FieldsToMapKey -> fieldsToMapPropertyValue))
      val fname = m(FirstNameKey).asInstanceOf[String]
      assert(fname == FirstName)

      val mname = m(MiddleNameKey).asInstanceOf[String]
      assert(mname == MiddleName)

      val lname = m(LastNameKey).asInstanceOf[String]
      assert(lname == LastName)
    }

    "return empty values for incorrect json path <-> record combinations" in {
      val m = fieldsToMapProcessor.mappings(Some(personWithNulls),
        Map(CoreProperties.FieldsToMapKey -> defaultFieldsToMapPropertyValue))

      val fname = m.get(FirstNameKey)
      assert(fname.isEmpty)

      val lname = m.get(LastNameKey)
      assert(lname.isEmpty)
    }
  }
}

object TestFieldsToMapProcessor {

  val sid = "org.dcs.api.PersonWithNameGroup"
  AvroSchemaStore.add(sid, this.getClass.getResourceAsStream("/avro/" + sid + ".avsc"))

  val NameSchemaKey = "name"
  val FirstNameKey = "first_name"
  val FirstNameSchemaKey = "first"
  val FirstName = "Obi"

  val MiddleNameKey = "middle_name"
  val MiddleNameSchemaKey = "middle"
  val MiddleName = "Wan"

  val LastNameKey = "last_name"
  val LastNameSchemaKey = "last"
  val LastName = "Kenobi"

  val AgeSchemaKey = "age"
  val Age = 9999

  val schema = AvroSchemaStore.get(sid)
  val name = new GenericData.Record(schema.get.getField("name").schema())
  name.put(FirstNameSchemaKey, FirstName)
  name.put(MiddleNameSchemaKey, MiddleName)
  name.put(LastNameSchemaKey, LastName)
  val person = new GenericData.Record(schema.get)
  person.put(NameSchemaKey, name)
  person.put(AgeSchemaKey, Age)

  val nameWithNulls = new GenericData.Record(schema.get.getField("name").schema())
  nameWithNulls.put(FirstNameSchemaKey, null)
  nameWithNulls.put(MiddleNameSchemaKey, MiddleName)
  nameWithNulls.put(LastNameSchemaKey, null)
  val personWithNulls = new GenericData.Record(schema.get)
  personWithNulls.put(NameSchemaKey, nameWithNulls)
  personWithNulls.put(AgeSchemaKey, Age)
}

class TestFieldsToMapProcessor extends Worker with FieldsToMap {
  import TestFieldsToMapProcessor._

  override def fields: List[String] = List(FirstNameKey, MiddleNameKey, LastNameKey)

  override def execute(record: Option[GenericRecord], properties: util.Map[String, String]): List[Either[ErrorResponse, (String, AnyRef)]] = {

    List(Right((RelationshipType.Success.id, record.get)))
  }

  override def metadata(): MetaData = MetaData("")


}