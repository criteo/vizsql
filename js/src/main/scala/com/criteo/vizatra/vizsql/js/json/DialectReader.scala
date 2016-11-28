package com.criteo.vizatra.vizsql.js.json

import com.criteo.vizatra.vizsql._
import com.criteo.vizatra.vizsql.hive.HiveDialect

import scala.scalajs.js.Dynamic

object DialectReader extends Reader[Dialect] {
  override def apply(dyn: Dynamic): Dialect = from(dyn.asInstanceOf[String])

  def from(input: String): Dialect = input.toLowerCase match {
    case "vertica" => vertica.dialect
    case "hsql" => hsqldb.dialect
    case "h2" => h2.dialect
    case "postgresql" => postgresql.dialect
    case "hive" => HiveDialect(Map.empty) // TODO: handle hive UDFs
    case _ => sql99.dialect
  }
}
