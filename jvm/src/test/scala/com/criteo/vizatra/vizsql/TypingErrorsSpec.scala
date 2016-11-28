package com.criteo.vizatra.vizsql

import sql99._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, EitherValues, PropSpec}

class TypingErrorsSpec extends PropSpec with Matchers with EitherValues {

  val invalidSQL99SelectStatements = TableDrivenPropertyChecks.Table(
    ("SQL", "Expected error"),

    (
      """select ?""",
      """|select ?
         |       ^
         |Error: parameter type cannot be inferred from context
      """
    ),
    (
      """select ? = ?""",
      """|select ? = ?
         |       ^
         |Error: parameter type cannot be inferred from context
      """
    ),
    (
      """select ? = plop""",
      """|select ? = plop
         |           ^
         |Error: column not found plop
      """
    ),
    (
      """select YOLO(88)""",
      """|select YOLO(88)
         |       ^
         |Error: unknown function yolo
      """
    ),
    (
      """select min(12, 13)""",
      """|select min(12, 13)
         |               ^
         |Error: too many arguments
      """
    ),
    (
      """select max()""",
      """|select max()
         |          ^
         |Error: expected argument
      """
    ),
    (
      """select ? between ? and ?""",
      """|select ? between ? and ?
         |       ^
         |Error: parameter type cannot be inferred from context
      """
    ),
    (
      """select ? between ? and coco""",
      """|select ? between ? and coco
         |                       ^
         |Error: column not found coco
      """
    ),
    (
      """select ? in (?)""",
      """|select ? in (?)
         |       ^
         |Error: parameter type cannot be inferred from context
      """
    ),
    (
      """select case when city_id = 1 then 'foo' else 1 end from City""",
      """|select case when city_id = 1 then 'foo' else 1 end from City
         |                                             ^
         |Error: expected string, found integer
      """
    ),
    (
      """select 1 = true""",
      """|select 1 = true
         |           ^
         |Error: expected integer, found boolean
      """
    ),
    (
      """select * from (select 1 a union select true a) x""",
      """|select * from (select 1 a union select true a) x
         |                                ^
         |Error: expected integer, found boolean for column a
      """
    ),
    (
      """select * from (select 1, 2 union select 1) x""",
      """|select * from (select 1, 2 union select 1) x
         |                                 ^
         |Error: expected same number of columns on both sides of the union
      """
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

  property("report typing errors on invalid SQL-99 SELECT statements") {
    TableDrivenPropertyChecks.forAll(invalidSQL99SelectStatements) {
      case (sql, expectedError) =>
        VizSQL.parseQuery(sql, SAKILA)
          .fold(
            e => sys.error(s"Query doesn't parse: $e"),
            _.error.map(_.toString(sql, ' ')).getOrElse(sys.error(s"Query should not type!"))
          ) should be (expectedError.stripMargin.trim)
    }
  }

}