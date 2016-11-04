package com.criteo.vizatra.vizsql.js

import com.criteo.vizatra.vizsql.DB
import com.criteo.vizatra.vizsql.js.json.DBReader

import scala.scalajs.js.Dynamic
import scala.scalajs.js.annotation.JSExport

@JSExport("VizSQL.Database")
object Database {
  @JSExport
  def from(input: Dynamic): DB = DBReader.apply(input)
}
