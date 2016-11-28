package com.criteo.vizatra.vizsql.hive

import com.criteo.vizatra.vizsql._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, PropSpec}

class HiveTypeParserSpec extends PropSpec with Matchers {

  val types = TableDrivenPropertyChecks.Table(
    ("Type string", "Expected type"),

    ("double", DECIMAL(true)),
    ("array<int>", HiveArray(INTEGER(true))),
    ("map<string,struct<a:boolean,b:timestamp>>", HiveMap(STRING(true), HiveStruct(List(
      Column("a", BOOLEAN(true)),
      Column("b", TIMESTAMP(true))
    )))),
    ("array<struct<foo:array<map<string,string>>,bar:array<int>,`baz`:double>>", HiveArray(HiveStruct(List(
      Column("foo", HiveArray(HiveMap(STRING(true), STRING(true)))),
      Column("bar", HiveArray(INTEGER(true))),
      Column("baz", DECIMAL(true))
    )))),
    ("struct<timestamp:integer>", HiveStruct(List(
      Column("timestamp", INTEGER(true))
    )))
  )

  // --

  property("parse to correct types") {
    val parser = new TypeParser
    TableDrivenPropertyChecks.forAll(types) {
      case (typeString, expectedType) =>
        parser.parseType(typeString) shouldEqual Right(expectedType)
    }
  }
}
