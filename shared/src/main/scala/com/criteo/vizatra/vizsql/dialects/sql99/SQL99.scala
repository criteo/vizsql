package com.criteo.vizatra.vizsql

object sql99 {
  implicit val dialect = new Dialect {
    lazy val parser = new SQL99Parser
    lazy val functions = SQLFunction.standard
    override def toString = "SQL-99"
  }
}
