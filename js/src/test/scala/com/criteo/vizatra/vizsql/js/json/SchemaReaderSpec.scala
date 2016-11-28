package com.criteo.vizatra.vizsql.js.json

import com.criteo.vizatra.vizsql.{Column, INTEGER, Schema, Table}
import org.scalatest.{FlatSpec, Matchers}

class SchemaReaderSpec extends FlatSpec with Matchers {
  "apply()" should "return a schema" in {
    val res = SchemaReader(
      """
        |{
        | "name":"schema1",
        | "tables": [
        |   {
        |     "name":"table1",
        |     "columns": [
        |       {"name": "col1", "type": "int4"}
        |     ]
        |   }
        | ]
        |}
      """.stripMargin)
    res shouldEqual Schema(
      "schema1",
      List(
        Table("table1", List(Column("col1", INTEGER())))
      )
    )
  }
}
