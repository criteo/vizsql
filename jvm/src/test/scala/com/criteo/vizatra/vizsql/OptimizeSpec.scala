package com.criteo.vizatra.vizsql

import org.scalatest.{Matchers, FlatSpec}

class OptimizeSpec extends FlatSpec with Matchers {

  val CASE_QUERY =
    """SELECT
      | country,
      | CASE 1 WHEN 1 THEN 13
      |   WHEN clicks THEN 9 END,
      | CASE 1 WHEN clicks THEN 65
      |   WHEN 1 THEN 12 END,
      | CASE clicks WHEN clicks THEN 1
      |   WHEN 42 then 77
      |   ELSE 18 END,
      | CASE WHEN 1 > 2 THEN 42
      |   WHEN 2 > 1 THEN 38
      |   WHEN CLICKS > DISPLAYS THEN 654 END,
      | CASE 5 WHEN 1 THEN 2
      |   WHEN 3 THEN 4
      |   ELSE 10 END,
      | CASE 5 WHEN 1 THEN 2
      |   WHEN 3 THEN 4
      |   END,
      | CASE 42 WHEN 10 THEN 2
      |   WHEN displays THEN 12
      |   ELSE 88
      |   END
      | FROM facts
    """.stripMargin

  val OPERATOR_QUERY =
    """SELECT
      |country,
      |3 + 2,
      |10/5,
      |NULL / 2,
      |3 * 4,
      |5 / 0,
      |'Marco' + 'Polo',
      |44 - 2,
      | -(-(-2))
      |FROM facts
      |WHERE (3 > 2) AND 'X' = 'Y'
    """.stripMargin

  val JOIN_QUERY =
    """SELECT
      |f.region,
      |f.country,
      |f.date,
      |c.comment
      |FROM facts AS f
      |JOIN review AS c ON f.idfacts = c.idfacts AND 2 > 3
    """.stripMargin

  val SUB_QUERY=
    """SELECT
      |x.result,
      |(SELECT comment FROM review WHERE idfacts = 42 AND (2 + 2 = 4)) AS comment
      |FROM (SELECT (2 + 2) AS result) AS x
      |WHERE (SELECT 2 = 2)
    """.stripMargin

  val testDb = {
    val cols = for { (name, coltype) <- List(("idfacts", INTEGER(false)),
      ("REGION", STRING(false)),
      ("COUNTRY", STRING(false)),
      ("CLICKS", INTEGER(false)),
      ("DISPLAYS", INTEGER(false)),
      ("DATE", DATE(true)))}
      yield Column(name, coltype)
    val cols2 = for { (name, coltype) <- List(("idfacts", INTEGER(false)),
      ("COMMENT", STRING(false)),
      ("COMMENTER", STRING(false)),
      ("RATING", INTEGER(true)))}
      yield Column(name, coltype)
    implicit val dialect = sql99.dialect
    DB(List(Schema("", List(Table("facts", cols), Table("review", cols2)))))
  }

  val subQuery = VizSQL.parseQuery(SUB_QUERY, testDb)
  val caseQuery = VizSQL.parseQuery(CASE_QUERY, testDb)
  val opQuery =  VizSQL.parseQuery(OPERATOR_QUERY, testDb)
  val joinQuery = VizSQL.parseQuery(JOIN_QUERY, testDb)

  val queries = List(subQuery, caseQuery, opQuery, joinQuery)

  "The Optimizer" should "Simplify \"CASE\" statements" in {
    caseQuery.isRight shouldBe true
    val expected =
      """SELECT
        |country,
        |13,
        |CASE 1 WHEN clicks THEN 65
        | WHEN 1 THEN 12 END,
        |CASE clicks WHEN clicks THEN 1
        | WHEN 42 then 77
        | ELSE 18 END,
        |38,
        |10,
        |NULL,
        |CASE 42
        | WHEN displays THEN 12
        | ELSE 88
        | END
        |FROM facts
      """.stripMargin
    val query = caseQuery.right.get
    val expectedQ = VizSQL.parseQuery(expected, testDb).right.get
    Optimizer.optimize(query).sql should be (expectedQ.select.toSQL)
  }

  it should "pre-compute literal expressions" in {
    val expected =
      """SELECT
        |country,
        |5,
        |2,
        |NULL,
        |12,
        |5 / 0,
        |'Marco' + 'Polo',
        |42,
        |-2
        |FROM facts
        |WHERE FALSE
      """.stripMargin
    opQuery.isRight shouldBe true
    val query = opQuery.right.get
    val expectedQ = VizSQL.parseQuery(expected, testDb).right.get
    Optimizer.optimize(query).sql should be (expectedQ.select.toSQL)
  }

  it should "be idempotent" in {
    for {q <- queries } {
      q.isRight shouldBe true
      val query = q.right.get
      val optimized = Optimizer.optimize(query)
      val superOptimized = Optimizer.optimize(optimized)
      superOptimized should be(optimized)
    }
  }

  it should "remove unnecessary tables" in {
    val QUERY =
      """SELECT
        |3.0 / 2
        |FROM facts
      """.stripMargin
    val expected =
      """SELECT
        |1.5
      """.stripMargin
    val query = VizSQL.parseQuery(QUERY, testDb).right.get
    val ref = VizSQL.parseQuery(expected, testDb).right.get
    Optimizer.optimize(query).sql should be (ref.select.toSQL)
    val QUERY2 =
      """SELECT
        |r.comment,
        |3.0 / 2
        |FROM facts as f
        |JOIN review as r ON r.idfact = f.idfact
      """.stripMargin
    val expected2 =
      """SELECT
        |r.comment,
        |1.5
        |FROM review as r
      """.stripMargin
    val query2 = VizSQL.parseQuery(QUERY2, testDb).right.get
    val ref2 = VizSQL.parseQuery(expected2, testDb).right.get
    Optimizer.optimize(query2).sql should be (ref2.select.toSQL)
  }

  it should "rewrite sons of expressions it can't optimize" in {
    val QUERY = """SELECT
                  |SUM(3.0 / 2),
                  |CAST(3 + 2 AS INTEGER) / 2,
                  |(5 > 3)
                  |FROM facts
                """.stripMargin
    val expected =
      """SELECT
        |SUM(1.5),
        |CAST(5 AS INTEGER) / 2,
        |TRUE
      """.stripMargin
    val query = VizSQL.parseQuery(QUERY, testDb).right.get
    val ref = VizSQL.parseQuery(expected, testDb).right.get
    Optimizer.optimize(query).sql should be (ref.select.toSQL)
  }

  it should "optimize joins" in {
    val expected =
      """SELECT
        |f.region,
        |f.country,
        |f.date,
        |c.comment
        |FROM facts AS f
        |JOIN review AS c ON f.idfacts = c.idfacts AND FALSE
      """.stripMargin
    val query = joinQuery.right.get
    val expectedQ = VizSQL.parseQuery(expected, testDb).right.get
    Optimizer.optimize(query).sql should be (expectedQ.select.toSQL)
  }

  it should "handle Subselect" in {
    val expected =
      """SELECT
        |x.result,
        |(SELECT comment FROM review WHERE idfacts = 42 AND TRUE) AS comment
        |FROM (SELECT 4 AS result) AS x
        |WHERE (SELECT TRUE)
      """.stripMargin
    val query = subQuery.right.get
    val expectedQ = VizSQL.parseQuery(expected, testDb).right.get
    Optimizer.optimize(query).sql should be (expectedQ.select.toSQL)
  }

}
