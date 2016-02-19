package com.criteo.vizatra.vizsql

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, EitherValues, PropSpec}
import sql99._

class ExtractColumnsSpec extends PropSpec with Matchers with EitherValues {

  val validSQL99SelectStatements = TableDrivenPropertyChecks.Table(
    ("SQL", "Expected Columns"),

    ("""SELECT 1""", List(
      Column("1", INTEGER(false))
    )),

    ("""SELECT 1 as one""", List(
      Column("one", INTEGER(false))
    )),

    ("""SELECT * from City""", List(
      Column("city_id", INTEGER(nullable = false)),
      Column("city", STRING(nullable = false)),
      Column("country_id", INTEGER(nullable = false)),
      Column("last_update", TIMESTAMP(nullable = false))
    )),

    ("""SELECT x.* from City as x""", List(
      Column("city_id", INTEGER(nullable = false)),
      Column("city", STRING(nullable = false)),
      Column("country_id", INTEGER(nullable = false)),
      Column("last_update", TIMESTAMP(nullable = false))
    )),

    ("""SELECT x.* from sakila.City x""", List(
      Column("city_id", INTEGER(nullable = false)),
      Column("city", STRING(nullable = false)),
      Column("country_id", INTEGER(nullable = false)),
      Column("last_update", TIMESTAMP(nullable = false))
    )),

    ("""SELECT country_id, max(last_update) from City as x""", List(
      Column("country_id", INTEGER(nullable = false)),
      Column("MAX(last_update)", TIMESTAMP(nullable = false))
    )),

    ("""SELECT cities.country_id, max(cities.last_update) from City as cities""", List(
      Column("cities.country_id", INTEGER(nullable = false)),
      Column("MAX(cities.last_update)", TIMESTAMP(nullable = false))
    )),

    ("""SELECT * from Country, City""", List(
      Column("country_id", INTEGER(nullable = false)),
      Column("country", STRING(nullable = false)),
      Column("last_update", TIMESTAMP(nullable = false)),
      Column("city_id", INTEGER(nullable = false)),
      Column("city", STRING(nullable = false)),
      Column("country_id", INTEGER(nullable = false)),
      Column("last_update", TIMESTAMP(nullable = false))
    )),

    ("""SELECT city_id from city""", List(
      Column("city_id", INTEGER(nullable = false))
    )),

    ("""SELECT City.country_id from Country, City""", List(
      Column("city.country_id", INTEGER(nullable = false))
    )),

    ("""select blah.* from (select *, 1 as one from Country) as blah;""", List(
      Column("country_id", INTEGER(nullable = false)),
      Column("country", STRING(nullable = false)),
      Column("last_update", TIMESTAMP(nullable = false)),
      Column("one", INTEGER(false))
    )),

    ("""select * from City as v join Country as p on v.country_id = p.country_id""", List(
      Column("city_id", INTEGER(nullable = false)),
      Column("city", STRING(nullable = false)),
      Column("country_id", INTEGER(nullable = false)),
      Column("last_update", TIMESTAMP(nullable = false)),
      Column("country_id", INTEGER(nullable = false)),
      Column("country", STRING(nullable = false)),
      Column("last_update", TIMESTAMP(nullable = false))
    )),

    ("SELECT now() as now, MAX(last_update) + 3599 / 86400 AS last_update FROM City", List(
      Column("now", TIMESTAMP(nullable = false)),
      Column("last_update", TIMESTAMP(nullable = false))
    )),

    ("""select case 3 when 1 then 2 when 3 then 5 else 0 end AS foo""", List(
      Column("foo", INTEGER(nullable = false))
    )),

    ("""select case 3 when 1 then 2 when 3 then 5 end AS foo""", List(
      Column("foo", INTEGER(nullable = true))
    )),

    ("""select case city when 'a' then 2.5 when 'b' then 5.0 end AS foo from City""", List(
      Column("foo", DECIMAL(nullable = true))
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

  property("extract SQL-99 SELECT statements columns") {
    TableDrivenPropertyChecks.forAll(validSQL99SelectStatements) {
      case (sql, expectedColumns) =>
        VizSQL.parseQuery(sql, SAKILA)
          .fold(e => sys.error(s"Query doesn't parse: $e"), identity)
          .columns
          .fold(e => sys.error(s"Invalid query: $e"), identity) should be (expectedColumns)
    }
  }

}