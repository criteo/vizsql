package com.criteo.vizatra.vizsql

object vertica {
  implicit val dialect = new Dialect {
    def parser = new SQL99Parser
    val functions = postgresql.dialect.functions
    override def toString = "Vertica"
  }
}
