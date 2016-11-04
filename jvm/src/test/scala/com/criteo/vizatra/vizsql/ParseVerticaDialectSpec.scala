package com.criteo.vizatra.vizsql

import com.criteo.vizatra.vizsql.vertica._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{EitherValues, Matchers, PropSpec}

class ParseVerticaDialectSpec extends PropSpec with Matchers with EitherValues {

  val validVerticaSelectStatements = TableDrivenPropertyChecks.Table(
    ("SQL", "Expected Columns"),
    ("""SELECT
       |   NOW() as now,
       |   MAX(last_update) + 3599 / 86400 AS last_update,
       |   CONCAT('The most recent update was on ', TO_CHAR(MAX(last_update) + 3599 / 86400, 'YYYY-MM-DD at HH:MI')) as content
       |FROM
       |    City""".stripMargin,
      List(
        Column("now", TIMESTAMP(nullable = false)),
        Column("last_update", TIMESTAMP(nullable = false)),
        Column("content", STRING(nullable = false))
      ))
  )

  // --

  val SAKILA = DB(schemas = List(
    Schema(
      "sakila",
      tables = List(
        Table(
          "City",
          columns = List(
            Column("city_id", INTEGER(nullable = false)),
            Column("city", STRING(nullable = false)),
            Column("country_id", INTEGER(nullable = false)),
            Column("last_update", TIMESTAMP(nullable = false))
          )
        ),
        Table(
          "Country",
          columns = List(
            Column("country_id", INTEGER(nullable = false)),
            Column("country", STRING(nullable = false)),
            Column("last_update", TIMESTAMP(nullable = false))
          )
        )
      )
    )
  ))

  // --

  property("extract Vertica SELECT statements columns") {
    TableDrivenPropertyChecks.forAll(validVerticaSelectStatements) {
      case (sql, expectedColumns) =>
        VizSQL.parseQuery(sql, SAKILA)
          .fold(e => sys.error(s"Query doesn't parse: $e"), identity)
          .columns
          .fold(e => sys.error(s"Invalid query: $e"), identity) should be (expectedColumns)
    }
  }

}