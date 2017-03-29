package org.dcs.api

import java.util

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.commons.io.IOUtils
import org.dcs.api.processor.CoreProperties._
import org.dcs.api.processor._
import org.dcs.commons.Control.using
import org.dcs.commons.error.ErrorResponse
import org.dcs.commons.serde.AvroSchemaStore
import org.dcs.commons.serde.JsonSerializerImplicits._

import scala.collection.JavaConverters._
import org.dcs.commons.serde.AvroImplicits._

/**
  * Created by cmathew on 27.03.17.
  */
class FieldActionsProcessorSpec  extends ApiUnitWordSpec {
  import TestFieldActionsProcessor._

  "Field Actions Processor" should {
    val fieldActionsprocessor = new TestFieldActionsProcessor
    val personSchema = AvroSchemaStore.get(sid)
    val personSchemaJson =
      using(this.getClass.getResourceAsStream("/avro/" + sid + ".avsc")) { is =>
        IOUtils.toString(is)
      }
    val input = person.serToBytes(personSchema)

    val defaultFieldActionsPropertyValue = List(Action("", ContainsCmd, ""), Action("", StartsWithCmd, "")).toJson

    "return correct default value for fields actions property" in {
      assertResult(defaultFieldActionsPropertyValue) {
        fieldActionsprocessor.properties().asScala.find(p => p.name == CoreProperties.FieldActionsKey).get.defaultValue
      }
    }

    val validFieldActionsPropertyValue = List(Action("$.name.first", ContainsCmd, "Ob")).toJson

    "return valid response for provided field actions" in {
      assertResult(TestFieldActionsProcessor.person) {
        fieldActionsprocessor.
          trigger(input,
            Map(ReadSchemaKey -> personSchemaJson, FieldActionsKey -> validFieldActionsPropertyValue).asJava)(1).
          deSerToGenericRecord(personSchema, personSchema)
      }
    }
  }

}

object TestFieldActionsProcessor {
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

  val ContainsCmd = "contains"
  val StartsWithCmd = "starts with"
}

class TestFieldActionsProcessor extends Worker with FieldActions {
  import TestFieldActionsProcessor._

  override def cmds: List[String] = List("contains", "starts with")

  override def execute(record: Option[GenericRecord], properties: util.Map[String, String]): List[Either[ErrorResponse, AnyRef]] = {
    val isValid: Boolean = actions(properties).map(a => a.cmd match {
      case ContainsCmd => a.fromJsonPath(record).value.asString.exists(s => s.contains(a.args))
      case StartsWithCmd => a.fromJsonPath(record).value.asString.exists(s => s.contains(a.args))
      case _ => false
    }).forall(identity)

    if(isValid)
      List(Right(record.get))
    else
      List(Right(null))
  }

  override def metadata(): MetaData = MetaData()
}
