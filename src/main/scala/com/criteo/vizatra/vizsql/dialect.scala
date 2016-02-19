package com.criteo.vizatra.vizsql

trait Dialect {
  def parser: SQLParser
  def functions: PartialFunction[String,SQLFunction]
}

object sql99 {
  implicit val dialect = new Dialect {
    def parser = new SQL99Parser
    val functions = SQLFunction.standard
    override def toString = "SQL-99"
  }
}

object vertica {
  implicit val dialect = new Dialect {
    def parser = new SQL99Parser
    val functions = postgresql.dialect.functions
    override def toString = "Vertica"
  }
}

object postgresql {
  implicit val dialect = new Dialect {
    def parser = new SQL99Parser
    val functions = SQLFunction.standard orElse {
      case "date_trunc" => new SQLFunction2 {
        def result = { case ((_, _), (_, t)) => Right(TIMESTAMP(nullable = t.nullable)) }
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

object hsqldb {
  implicit val dialect = new Dialect {
    def parser = new SQL99Parser
    val functions = SQLFunction.standard orElse {
      case "timestamp" => new SQLFunction2 {
        def result = { case ((_, t1), (_, t2)) => Right(TIMESTAMP(nullable = t1.nullable || t2.nullable)) }
      }
    }: PartialFunction[String,SQLFunction]
    override def toString = "HSQLDB"
  }
}

object h2 {
  implicit val dialect = new Dialect {
    def parser = new SQL99Parser
    val functions = SQLFunction.standard
    override def toString = "H2"
  }
}