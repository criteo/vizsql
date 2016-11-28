package com.criteo.vizatra.vizsql.js.json

import com.criteo.vizatra.vizsql._
import org.scalatest.{FlatSpec, Matchers}

class DBReaderSpec extends FlatSpec with Matchers {
  "apply()" should "returns a DB" in {
    val db = DBReader.apply(
      """
        |{
        | "dialect": "vertica",
        | "schemas": [
        |   {
        |     "name":"schema1",
        |     "tables": [
        |       {
        |         "name":"table1",
        |         "columns": [
        |           {"name": "col1", "type": "int4"}
        |         ]
        |       }
        |     ]
        |   }
        | ]
        |}
      """.stripMargin)
    db.dialect shouldBe vertica.dialect
    db.schemas shouldEqual Schemas(List(Schema("schema1", List(Table("table1", List(Column("col1", INTEGER())))))))
  }
}
