package com.criteo.vizatra.vizsql

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{EitherValues, Matchers, PropSpec}
import sql99._

/**
  * Test cases for schema errors
  */
class SchemaErrorsSpec extends PropSpec with Matchers with EitherValues {

    val invalidSQL99SelectStatements = TableDrivenPropertyChecks.Table(
      ("SQL", "Expected error"),
      (
        "SELECT region from City as C1 JOIN Country as C2 ON C1.country_id = C2.country_id WHERE region < 42",
        SchemaError("ambiguous column region", 6)
      ),(
        "SELECT nonexistent, region from City",
        SchemaError("column not found nonexistent", 6)
        )
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
              Column("last_update", TIMESTAMP(nullable = false)),
              Column("region", INTEGER(nullable = false))
            )
          ),
          Table(
            "Country",
            columns = List(
              Column("country_id", INTEGER(nullable = false)),
              Column("country", STRING(nullable = false)),
              Column("last_update", TIMESTAMP(nullable = false)),
              Column("region", INTEGER(nullable = false))
            )
          )
        )
      )
    ))

    // --

    property("report schema errors on invalid SQL-99 SELECT statements") {
      TableDrivenPropertyChecks.forAll(invalidSQL99SelectStatements) {
        case (sql, expectedError) =>
          VizSQL.parseQuery(sql, SAKILA)
            .fold(
              e => sys.error(s"Query doesn't parse: $e"),
              _.error.getOrElse(sys.error(s"Query should not type!"))
            ) should be (expectedError)
      }
    }

  }
