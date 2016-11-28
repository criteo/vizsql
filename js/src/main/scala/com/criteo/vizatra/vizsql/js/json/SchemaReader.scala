package com.criteo.vizatra.vizsql.js.json

import com.criteo.vizatra.vizsql.{Schema, Table}
import scalajs.js.Array

import scala.scalajs.js.UndefOr
import scala.scalajs.js.Dynamic

object SchemaReader extends Reader[Schema] {
  override def apply(dyn: Dynamic): Schema = {
    val name = dyn.name.asInstanceOf[String]
    val tables: List[Table] = dyn.tables
      .asInstanceOf[UndefOr[Array[Dynamic]]]
      .getOrElse(Array())
      .toList map TableReader.apply
    Schema(name, tables)
  }
}
