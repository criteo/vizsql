package com.criteo.vizatra.vizsql.js.json

import com.criteo.vizatra.vizsql.hive.{HiveArray, HiveStruct}
import com.criteo.vizatra.vizsql.{BOOLEAN, Column, INTEGER, STRING}
import org.scalatest.{FlatSpec, Matchers}

class ColumnReaderSpec extends FlatSpec with Matchers {
  "apply()" should "return a column with nullable" in {
    val res = ColumnReader.apply("""{"name":"col","type":"int4","nullable":true}""")
    res shouldEqual Column("col", INTEGER(true))
  }
  "apply()" should "return a column without nullable" in {
    val res = ColumnReader.apply("""{"name":"col","type":"int4"}""")
    res shouldEqual Column("col", INTEGER(false))
  }
  "parseType()" should "parse types from string" in {
    ColumnReader.parseType("array<integer>", true) shouldEqual HiveArray(INTEGER(true))
    ColumnReader.parseType("struct<a:string,b:boolean>", true) shouldEqual HiveStruct(List(
      Column("a", STRING(true)),
      Column("b", BOOLEAN(true))
    ))
  }
  "parseStructCols()" should "parse struct cols" in {
    ColumnReader.parseStructCols("a:string,b:boolean") shouldEqual List(
      Column("a", STRING(true)),
      Column("b", BOOLEAN(true))
    )
  }
}
