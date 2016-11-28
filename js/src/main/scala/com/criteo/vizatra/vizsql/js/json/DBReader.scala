package com.criteo.vizatra.vizsql.js.json

import com.criteo.vizatra.vizsql.{DB, Schemas}

import scala.scalajs.js.{Array, Dynamic, UndefOr}

object DBReader extends Reader[DB] {
  override def apply(dyn: Dynamic): DB = {
    implicit val dialect = DialectReader(dyn.dialect)
    val schemas = dyn.schemas
      .asInstanceOf[Array[Dynamic]]
      .toList map SchemaReader.apply
    DB(schemas)
  }
}
