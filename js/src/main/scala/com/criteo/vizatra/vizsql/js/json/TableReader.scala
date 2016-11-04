package com.criteo.vizatra.vizsql.js.json

import com.criteo.vizatra.vizsql.{Column, Table}

import scala.scalajs.js.{Dynamic, UndefOr, Array}

object TableReader extends Reader[Table] {
  override def apply(dyn: Dynamic): Table = {
    val table = dyn.name.asInstanceOf[String]
    val columns: List[Column] = dyn.columns
      .asInstanceOf[UndefOr[Array[Dynamic]]]
      .getOrElse(Array())
      .toList map ColumnReader.apply
    Table(table, columns)
  }
}
