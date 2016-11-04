package com.criteo.vizatra.vizsql.js

import com.criteo.vizatra.vizsql.js.common._
import com.criteo.vizatra.vizsql.{DB, Query, VizSQL}

import scala.scalajs.js.JSConverters._
import scala.scalajs.js.annotation.JSExport

@JSExport("VizSQL.QueryParser")
object QueryParser {
  @JSExport
  def parse(query: String, db: DB): ParseResult =
    VizSQL.parseQuery(query, db) match {
      case Left(err) => new ParseResult(new ParseError(err.msg, err.pos))
      case Right(query) => convert(query)
    }

  def convert(query: Query): ParseResult = {
    val select = query.select
    val db = query.db
    val result = for {
      columns <- select.getColumns(db).right
      tables <- select.getTables(db).right
    } yield (columns, tables)
    result fold (
      err => new ParseResult(new ParseError(err.msg, err.pos)), { case (columns, tables) =>
        val cols = columns map Column.from
        val tbls = tables map { case (maybeSchema, table) => Table.from(table, maybeSchema) }
        new ParseResult(select = new Select(cols.toJSArray, tbls.toJSArray))
      }
    )
  }
}
