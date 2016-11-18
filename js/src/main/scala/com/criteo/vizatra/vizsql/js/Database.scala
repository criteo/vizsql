package com.criteo.vizatra.vizsql.js

import com.criteo.vizatra.vizsql.DB
import com.criteo.vizatra.vizsql.js.json.DBReader

import scala.scalajs.js.Dynamic
import scala.scalajs.js
import scala.scalajs.js.annotation.{JSExport, ScalaJSDefined}

@JSExport("VizSQL.Database")
object Database {
  @JSExport
  def from(input: Dynamic): DB = DBReader.apply(input)
}

@ScalaJSDefined
class Database(db: DB) extends js.Object {
  def parse(query: String) = {
    QueryParser.parse(query, db)
  }
}
