package com.criteo.vizatra.vizsql

trait Dialect {
  def parser: SQLParser
  def functions: PartialFunction[String,SQLFunction]
}
