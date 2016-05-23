package com.criteo.vizatra.vizsql

object sql99 {
  implicit val dialect = new Dialect {
    def parser = new SQL99Parser
    val functions = SQLFunction.standard
    override def toString = "SQL-99"
  }
}
