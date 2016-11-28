package com.criteo.vizatra.vizsql.js.common

import scala.scalajs.js
import scala.scalajs.js.UndefOr
import scala.scalajs.js.annotation.ScalaJSDefined
import scala.scalajs.js.JSConverters._
import com.criteo.vizatra.vizsql

@ScalaJSDefined
class ParseResult(val error: UndefOr[ParseError] = js.undefined, val select: UndefOr[Select] = js.undefined) extends js.Object

@ScalaJSDefined
class ParseError(val msg: String, val pos: Int) extends js.Object

@ScalaJSDefined
class Select(val columns: js.Array[Column], val tables: js.Array[Table]) extends js.Object

@ScalaJSDefined
class Column(val name: String, val `type`: String, val nullable: Boolean) extends js.Object

object Column {
  def from(column: vizsql.Column): Column = new Column(column.name, column.typ.show, column.typ.nullable)
}

@ScalaJSDefined
class Table(val name: String, val columns: js.Array[Column], val schema: UndefOr[String] = js.undefined) extends js.Object

object Table {
  def from(table: vizsql.Table, schema: Option[String] = None): Table = new Table(table.name, table.columns.map(Column.from).toJSArray, schema.orUndefined)
}
