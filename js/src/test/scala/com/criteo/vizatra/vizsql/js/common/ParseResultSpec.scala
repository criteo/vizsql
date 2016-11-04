package com.criteo.vizatra.vizsql.js.common

import org.scalatest.{FunSpec, Matchers}
import com.criteo.vizatra.vizsql
import com.criteo.vizatra.vizsql.INTEGER

import scala.scalajs.js.JSON
class ParseResultSpec extends FunSpec with Matchers {
  describe("Column") {
    describe("from()") {
      it("converts scala Column to JS object") {
        val col = Column.from(vizsql.Column("col1", INTEGER(true)))
        JSON.stringify(col) shouldEqual """{"name":"col1","type":"integer","nullable":true}"""
      }
    }
  }

  describe("Table") {
    describe("from()") {
      it("converts scala Table to JS object") {
        val table = Table.from(vizsql.Table("table1", List(vizsql.Column("col1", INTEGER(true)))), Some("schema1"))
        JSON.stringify(table) shouldEqual """{"name":"table1","columns":[{"name":"col1","type":"integer","nullable":true}],"schema":"schema1"}"""
      }
    }
  }
}
