package com.criteo.vizatra.vizsql.js.json

import com.criteo.vizatra.vizsql.{Column, INTEGER}
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
}
