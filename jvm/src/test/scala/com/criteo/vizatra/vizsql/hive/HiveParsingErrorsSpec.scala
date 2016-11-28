package com.criteo.vizatra.vizsql.hive

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{EitherValues, Matchers, PropSpec}

class HiveParsingErrorsSpec extends PropSpec with Matchers with EitherValues {

  val invalidSQL99SelectStatements = TableDrivenPropertyChecks.Table(
    ("SQL", "Expected error"),

    (
      "select bucket from t",
      """select bucket from t
        |       ^
        |Error: *, table or expression expected
      """
    ),
    (
      "select foo from tbl limit 100 order by foo",
      """select foo from tbl limit 100 order by foo
        |                              ^
        |Error: ; expected
      """
    ),
    (
      "select foo from bar tablesample (bucket 2 out af 3)",
      """select foo from bar tablesample (bucket 2 out af 3)
        |                                              ^
        |Error: of expected
      """.stripMargin
    )
  )

  // --

  property("report parsing errors on invalid Hive statements") {
    TableDrivenPropertyChecks.forAll(invalidSQL99SelectStatements) {
      case (sql, expectedError) =>
        new HiveDialect(Map.empty).parser.parseStatement(sql)
          .fold(_.toString(sql, ' ').trim, _ => "[NO ERROR]") should be (expectedError.toString.stripMargin.trim)
    }
  }

}
