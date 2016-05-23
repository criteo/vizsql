package com.criteo.vizatra.vizsql

object mysql {
  implicit val dialect = new Dialect {
    def parser = new SQL99Parser
    val functions = SQLFunction.standard
    override def toString = "MySQL"
  }
}
