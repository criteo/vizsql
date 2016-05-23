package com.criteo.vizatra.vizsql

object h2 {
  implicit val dialect = new Dialect {
    def parser = new SQL99Parser
    val functions = SQLFunction.standard
    override def toString = "H2"
  }
}
