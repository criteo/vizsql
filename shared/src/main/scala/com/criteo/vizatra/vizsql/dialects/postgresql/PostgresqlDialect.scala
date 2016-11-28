package com.criteo.vizatra.vizsql

object postgresql {
  implicit val dialect = new Dialect {
    lazy val parser = new SQL99Parser
    lazy val functions = SQLFunction.standard orElse {
      case "date_trunc" => new SQLFunction2 {
        def result = { case (_, (_, t)) => Right(TIMESTAMP(nullable = t.nullable)) }
      }
      case "zeroifnull" => new SQLFunction1 {
        def result = { case (_, t) => Right(t.withNullable(false)) }
      }
      case "nullifzero" => new SQLFunction1 {
        def result = { case (_, t) => Right(t.withNullable(true)) }
      }
      case "nullif" => new SQLFunction2 {
        def result = { case ((_, t1), (_, t2)) => Right(t1.withNullable(true)) }
      }
      case "to_char" => new SQLFunction2 {
        def result  = { case ((_, t1), (_, t2)) => Right(STRING()) }
      }
    }: PartialFunction[String,SQLFunction]
    override def toString = "Postgres"
  }
}
