package com.criteo.vizatra.vizsql.js

import com.criteo.vizatra.vizsql.DB
import com.criteo.vizatra.vizsql._
import org.scalatest.{FlatSpec, Matchers}

class DatabaseSpec extends FlatSpec with Matchers {
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
        )
      )
    )
  ))

  "Database class" should "be able to parse queries" in {
    val database = new Database(db)
    val res = database.parse("SELECT city_id FROM city")
    res.select.get.columns.head.name shouldEqual "city_id"
  }
}
