package com.criteo.vizatra.vizsql.js.json

import com.criteo.vizatra.vizsql._
import com.criteo.vizatra.vizsql.hive.HiveDialect
import org.scalatest.{FlatSpec, Matchers}

class DialectReaderSpec extends FlatSpec with Matchers {
  "from()" should "returns a dialect" in {
    DialectReader.from("VERTICA") shouldBe vertica.dialect
    DialectReader.from("PostgreSQL") shouldBe postgresql.dialect
    DialectReader.from("any") shouldBe sql99.dialect
    DialectReader.from("hsql") shouldBe hsqldb.dialect
    DialectReader.from("h2") shouldBe h2.dialect
    DialectReader.from("Hive") shouldBe HiveDialect(Map.empty)
  }
}
