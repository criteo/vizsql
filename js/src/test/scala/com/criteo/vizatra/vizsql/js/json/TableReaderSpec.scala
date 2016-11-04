package com.criteo.vizatra.vizsql.js.json

import com.criteo.vizatra.vizsql.{Column, DECIMAL, INTEGER}
import org.scalatest.{FlatSpec, Matchers}

class TableReaderSpec extends FlatSpec with Matchers {
  "apply()" should "return a table" in {
    val res = TableReader.apply(
      """
        |{
        |"name": "table_1",
        |"columns": [
        | {
        |   "name": "col1",
        |   "type": "int4"
        | },
        | {
        |   "name": "col2",
        |   "type": "float4"
        | }
        |]
        |}""".stripMargin)
    res.name shouldBe "table_1"
    res.columns shouldBe List(Column("col1", INTEGER()), Column("col2", DECIMAL()))
  }
}
