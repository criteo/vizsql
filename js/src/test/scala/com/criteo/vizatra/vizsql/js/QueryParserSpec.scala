package com.criteo.vizatra.vizsql.js

import com.criteo.vizatra.vizsql._
import org.scalatest.{FlatSpec, Matchers}

class QueryParserSpec extends FlatSpec with Matchers {
  implicit val dialect = sql99.dialect
  val db = DB(schemas = List(
    Schema(
      "sakila",
      tables = List(
        Table(
          "City",
          columns = List(
            Column("city_id", INTEGER(nullable = false)),
            Column("city", STRING(nullable = true)),
            Column("country_id", INTEGER(nullable = false)),
            Column("last_update", TIMESTAMP(nullable = false))
          )
        ),
        Table(
          "Country",
          columns = List(
            Column("country_id", INTEGER(nullable = false)),
            Column("country", STRING(nullable = true)),
            Column("last_update", TIMESTAMP(nullable = false))
          )
        )
      )
    )
  ))
  "parse()" should "return a result" in {
    val result = QueryParser.parse(
      s"""
        |SELECT country, city
        |FROM city JOIN country ON city.country_id = country.country_id
        |WHERE city IN ?{availableCities}
      """.stripMargin, db)
    result.error.isDefined shouldBe false
    result.select.isDefined shouldBe true
    val select = result.select.get
    select.columns.length shouldBe 2
  }

  "parse()" should "handle errors" in {
    val result = QueryParser.parse(
      s"""S
      """.stripMargin, db)
    result.error.isDefined shouldBe true
    result.select.isDefined shouldBe false
    val error = result.error.get
    error.pos shouldBe 0
    error.msg shouldBe "select expected"
  }
  "parse()" should "identify invalid columns" in {
    val result = QueryParser.parse(
      s"""
         |SELECT country1, city
         |FROM city JOIN country ON city.country_id = country.country_id
         |WHERE city IN ?{availableCities}
      """.stripMargin, db)
    result.error.get.msg shouldBe "column not found country1"
  }
}
