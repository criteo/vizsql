package com.criteo.vizatra.vizsql.js

import com.criteo.vizatra.vizsql.DB
import com.criteo.vizatra.vizsql.js.json.DBReader

import scala.scalajs.js.Dynamic
import scala.scalajs.js
import scala.scalajs.js.annotation.{JSExport, ScalaJSDefined}

@JSExport("Database")
object Database {
  /**
    * Parses to a VizSQL DB object
    * @param input a JS object of the database definition
    * @return DB
    */
  @JSExport
  def parse(input: Dynamic): DB = DBReader.apply(input)

  /**
    * Construct a Database object from the database definition
    * @param input a JS object of the database definition
    * @return Database
    */
  @JSExport
  def from(input: Dynamic): Database = new Database(parse(input))
}

@ScalaJSDefined
class Database(db: DB) extends js.Object {
  def parse(query: String) = {
    QueryParser.parse(query, db)
  }
}
