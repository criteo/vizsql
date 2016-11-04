package com.criteo.vizatra.vizsql

import sql99._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, EitherValues, PropSpec}

class ExtractPlaceholdersSpec extends PropSpec with Matchers with EitherValues {

  val validSQL99SelectStatements = TableDrivenPropertyChecks.Table(
    ("SQL", "Expected Placeholders"),

    ("""SELECT * FROM city WHERE ?""", List(
      (None, BOOLEAN())
    )),

    ("""SELECT * FROM city WHERE ?condition""", List(
      (Some("condition"), BOOLEAN())
    )),

    ("""SELECT ?today:timestamp""", List(
      (Some("today"), TIMESTAMP())
    )),

    ("""SELECT ?today:timestamp""", List(
      (Some("today"), TIMESTAMP())
    )),

    ("""SELECT * FROM country WHERE country_id = ?""", List(
      (None, INTEGER())
    )),

    ("""select *, country like ? from country where country_id = ? and last_update < ?;""", List(
      (None, STRING(nullable = true)),
      (None, INTEGER()),
      (None, TIMESTAMP())
    )),

    ("""SELECT ?a:DECIMAL = ?b:DECIMAL""", List(
      (Some("a"), DECIMAL()),
      (Some("b"), DECIMAL())
    )),

    ("""SELECT max(?today:timestamp)""", List(
      (Some("today"), TIMESTAMP())
    )),

    ("""SELECT ? = 3""", List(
      (None, INTEGER())
    )),

    ("""SELECT 3 = ?""", List(
      (None, INTEGER())
    )),

    ("""SELECT ?:varchar = ?""", List(
      (None, STRING()), (None, STRING())
    )),

    ("""SELECT ? = ?:varchar""", List(
      (None, STRING()), (None, STRING())
    )),

    (
      """
      select
        *, ?today:timestamp as TODAY, ?ratio * city_id
      from city
      where
        1 + ?a > 10 + ?b:integer AND
        ?
        OR ?blah like city;
      """, List(
      (Some("today"), TIMESTAMP()),
      (Some("ratio"), DECIMAL()),
      (Some("a"), DECIMAL()),
      (Some("b"), INTEGER()),
      (None, BOOLEAN()),
      (Some("blah"), STRING(nullable = true))
    )),

    ("""select 1 between ? and ?""", List(
      (None, INTEGER()), (None, INTEGER())
    )),

    ("""select 1 between 0 and ?""", List(
      (None, INTEGER())
    )),

    ("""select 1 between ?[)""", List(
      (None, RANGE(INTEGER()))
    )),

    ("""select 1 between ?[:varchar)""", List(
      (None, RANGE(STRING()))
    )),

    ("""select ? between 0 and 5""", List(
      (None, INTEGER())
    )),

    ("""select ? between 'a' and ?""", List(
      (None, STRING()), (None, STRING())
    )),

    ("""select ? between ? and ?:timestamp""", List(
      (None, TIMESTAMP()), (None, TIMESTAMP()), (None, TIMESTAMP())
    )),

    ("""select * from city where last_update between ? and ?""", List(
      (None, TIMESTAMP()), (None, TIMESTAMP())
    )),

    ("""select ? between ? and (? + ?)""", List(
      (None, DECIMAL()), (None, DECIMAL()), (None, DECIMAL()), (None, DECIMAL())
    )),

    ("""select 1 in (?)""", List(
      (None, INTEGER())
    )),

    ("""select ? in (1,2,3,4)""", List(
      (None, INTEGER())
    )),

    ("""select ? in ('a','b',?,'d')""", List(
      (None, STRING()), (None, STRING())
    )),

    ("""select country_id in (?,?,?) from city""", List(
      (None, INTEGER()), (None, INTEGER()), (None, INTEGER())
    )),

    ("""select 1 in ?{}""", List(
      (None, SET(INTEGER()))
    )),

    (
      """
        select country in ?{authorizedCountries}
        from city join country on city.country_id = country.country_id
      """, List(
      (Some("authorizedCountries"), SET(STRING(nullable = true)))
    )),

    (
      """
        select city
        from city join country on city.country_id = ?
      """, List(
      (None, INTEGER())
    )),

    (
      """
        select
          case city_id
            when ? then 1
            when ? then 2
            else 0
          end
        from city
      """, List(
      (None, INTEGER()), (None, INTEGER())
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

  // --

  property("compute SQL-99 SELECT statements placeholders") {
    TableDrivenPropertyChecks.forAll(validSQL99SelectStatements) {
      case (sql, expectedPlaceholders) =>
        VizSQL.parseQuery(sql, SAKILA)
          .fold(e => sys.error(s"Query doesn't parse: $e"), identity)
          .placeholders
          .fold(e => sys.error(s"Invalid query: $e"), _.namesAndTypes) should be (expectedPlaceholders)
    }
  }

}