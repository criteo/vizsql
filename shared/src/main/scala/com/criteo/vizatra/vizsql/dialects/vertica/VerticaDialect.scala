package com.criteo.vizatra.vizsql

object vertica {
  implicit val dialect = new Dialect {
    lazy val parser = new SQL99Parser
    lazy val functions = postgresql.dialect.functions orElse {
      case "datediff" => new SQLFunction3 {
        def result = { case (_, (_, t1), (_, t2)) => Right(INTEGER(nullable = t1.nullable ||t2.nullable)) }
      }
      case "to_timestamp" => new SQLFunction1 {
        def result = { case (_, t1) => Right(TIMESTAMP(nullable = t1.nullable)) }
      }
    }: PartialFunction[String,SQLFunction]
    override def toString = "Vertica"
  }
}
