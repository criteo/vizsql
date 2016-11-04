package com.criteo.vizatra.vizsql.hive

import com.criteo.vizatra.vizsql._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, PropSpec}

class ParseHiveQuerySpec extends PropSpec with Matchers {
  val queries = TableDrivenPropertyChecks.Table(
    ("Valid Hive query", "Expected AST"),

    ("select a.b[2].c.d.e['foo'].f.g.h.i[0] from t",
      SimpleSelect(
        projections = List(ExpressionProjection(
          MapOrArrayAccessExpression(
            StructAccessExpr(
              StructAccessExpr(
                StructAccessExpr(
                  StructAccessExpr(
                    MapOrArrayAccessExpression(
                      StructAccessExpr(
                        StructAccessExpr(
                          StructAccessExpr(
                            MapOrArrayAccessExpression(
                              ColumnOrStructAccessExpression(ColumnIdent("b", Some(TableIdent("a")))),
                              LiteralExpression(IntegerLiteral(2))),
                            "c"),
                          "d"),
                        "e"),
                      LiteralExpression(StringLiteral("foo"))),
                    "f"),
                  "g"),
                "h"),
              "i"),
            LiteralExpression(IntegerLiteral(0)))
        )),
        relations = List(SingleTableRelation(TableIdent("t")))
      )
    ),
    ("select a, b from table x lateral view explode (col) y as z",
      SimpleSelect(
        projections = List(
          ExpressionProjection(ColumnOrStructAccessExpression(ColumnIdent("a"))),
          ExpressionProjection(ColumnOrStructAccessExpression(ColumnIdent("b")))
        ),
        relations = List(LateralView(
          inner = SingleTableRelation(TableIdent("table"), Some("x")),
          explodeFunction = FunctionCallExpression(
            name = "explode",
            distinct = None,
            args = List(ColumnOrStructAccessExpression(ColumnIdent("col")))),
          tableAlias = "y",
          columnAliases = List("z")
        ))
      )
    ),
    ("select * from ta a left semi join tb b on a.id = b.id",
      SimpleSelect(
        projections = List(AllColumns),
        relations = List(JoinRelation(
          left = SingleTableRelation(TableIdent("ta"), Some("a")),
          join = LeftSemiJoin,
          right = SingleTableRelation(TableIdent("tb"), Some("b")),
          on = Some(ComparisonExpression(
            op = "=",
            left = ColumnOrStructAccessExpression(ColumnIdent("id", Some(TableIdent("a")))),
            right = ColumnOrStructAccessExpression(ColumnIdent("id", Some(TableIdent("b"))))
          ))
        ))
      )
    ),
    ("select derp(a) from t",
      SimpleSelect(
        projections = List(ExpressionProjection(
          FunctionCallExpression(
            name = "derp",
            distinct = None,
            args = ColumnOrStructAccessExpression(ColumnIdent("a")) :: Nil
          )
        )),
        relations = List(SingleTableRelation(TableIdent("t")))
      )
    ),
    ("select `select` s, `from` f from `join` j order by `select` desc cluster by `from` limit 100",
      //FIXME limit is lost, and cluster by is clumped with order by
      SimpleSelect(
        projections = List(
          ExpressionProjection(ColumnOrStructAccessExpression(ColumnIdent("select")), Some("s")),
          ExpressionProjection(ColumnOrStructAccessExpression(ColumnIdent("from")), Some("f"))
        ),
        relations = List(SingleTableRelation(TableIdent("join"), Some("j"))),
        orderBy = List(
          SortExpression(ColumnOrStructAccessExpression(ColumnIdent("select")), Some(SortDESC)),
          SortExpression(ColumnOrStructAccessExpression(ColumnIdent("from")), None)
        ),
        limit = Some(IntegerLiteral(100))
      )
    ),
    ("select foo from bar tablesample (bucket 2 out of 3 on baz)",
      //FIXME tablesample is lost
      SimpleSelect(
        projections = List(ExpressionProjection (ColumnOrStructAccessExpression(ColumnIdent("foo")))),
        relations = List(SingleTableRelation(TableIdent("bar")))
      )
    )
  )

  // --

  property("parse query") {
    TableDrivenPropertyChecks.forAll(queries) { case (query, ast) =>
      new HiveDialect(Map.empty).parser.parseStatement(query) match {
        case Left(err) => fail(err.toString(query))
        case Right(result) => result shouldEqual ast
      }
    }
  }
}
