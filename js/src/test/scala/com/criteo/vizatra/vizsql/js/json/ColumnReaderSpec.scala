package com.criteo.vizatra.vizsql.js.json

import com.criteo.vizatra.vizsql.hive.{HiveArray, HiveMap, HiveStruct, TypeParser}
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
  "parseType()" should "parse map type" in {
    val res = ColumnReader.parseType("""map<string,integer>""", true)
    res shouldEqual HiveMap(STRING(true), INTEGER(true))
  }
  "parseType()" should "parse array type" in {
    ColumnReader.parseType("array<int>", true) shouldEqual HiveArray(INTEGER(true))
  }
  "parseType()" should "parse struct type" in {
    ColumnReader.parseType("struct<a:struct<b:boolean>,c:struct<d:string>>", true) shouldEqual HiveStruct(List(
      Column("a", HiveStruct(List(
        Column("b", BOOLEAN(true))
      ))),
      Column("c", HiveStruct(List(
        Column("d", STRING(true))
      )))
    ))
  }
}
