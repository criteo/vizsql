package com.criteo.vizatra.vizsql

case class Schemas(schemas: List[Schema]) {
  val index = schemas.map(s => (s.name.toLowerCase, s)).toMap

  def getSchema(name: String): Either[String, Schema] = {
    index.get(name.toLowerCase).map(Right.apply _).getOrElse {
      Left(s"""schema not found $name""")
    }
  }

  def getNonAmbiguousTable(name: String): Either[String, (Schema, Table)] = {
    schemas.flatMap(s => s.getTable(name).right.toOption.map(s -> _)) match {
      case Nil => Left(s"table not found $name")
      case table :: Nil => Right(table)
      case _ => Left(s"ambiguous table $name")
    }
  }

  def getNonAmbiguousColumn(name: String): Either[String, (Schema, Table, Column)] = {
    schemas.map(s => s.getNonAmbiguousColumn(name).right.map { case (t, c) => (s, t, c) }) match {
      case Right(col) :: Nil => Right(col)
      case Left(err) :: _ => Left(err)
      case _ => Left(s"column not found $name")
    }
  }
}

case class DB(dialect: Dialect, schemas: Schemas, view: Schemas = Schemas(Nil)) {

  def function(name: String): Either[String, SQLFunction] = {
    dialect.functions.lift(name).map(Right.apply).getOrElse(Left(s"unknown function $name"))
  }

}

object DB {
  def apply(schemas: List[Schema])(implicit dialect: Dialect): DB = DB(dialect, Schemas(schemas))

  import java.sql.{Connection, ResultSet}

  def apply(connection: Connection): DB = {
    val databaseMetaData = connection.getMetaData()

    val name = databaseMetaData.getDatabaseProductName()
    val version = databaseMetaData.getDatabaseProductVersion()

    val dialect = (name, version) match {
      case ("Vertica Database", _) => vertica.dialect
      case ("HSQL Database Engine", _) => hsqldb.dialect
      case ("H2", _) => h2.dialect
      case ("PostgreSQL", _) => postgresql.dialect
      case x => sql99.dialect
    }

    def consume(rs: ResultSet): List[Map[String, Any]] = {
      import collection.mutable._
      val data = ListBuffer.empty[Map[String, Any]]
      while (rs.next()) {
        val row = HashMap.empty[String, Any]
        (1 to rs.getMetaData.getColumnCount).foreach { i =>
          rs.getObject(i).asInstanceOf[Any] match {
            case null =>
            case x => row.put(rs.getMetaData.getColumnLabel(i).toLowerCase, x)
          }
        }
        data += row
      }
      data.toList.map(_.toMap)
    }

    val allColumns = consume(databaseMetaData.getColumns(null, null, null, null))

    DB(
      dialect,
      Schemas(
        allColumns.flatMap(_.get("table_schem").map(_.toString.toLowerCase)).distinct.map { schemaName =>
          val schemaColumns = allColumns.filter(_.get("table_schem").map(_.toString.toLowerCase) == Some(schemaName)).flatMap { col =>
            for {
              table <- col.get("table_name").map(_.toString.toLowerCase)
              name <- col.get("column_name").map(_.toString.toLowerCase)
              nullable <- col.get("is_nullable").map(_.toString.toLowerCase).collect {
                case "yes" => true
                case "no" => false
              }
              typ <- col.get("type_name").map(_.toString.toLowerCase).collect(Type.from(nullable))
            } yield (table, Column(name, typ))
          }
          Schema(
            name = schemaName,
            tables = schemaColumns.groupBy(_._1).map {
              case (tableName, columns) => Table(tableName, columns.map(_._2))
            }.toList
          )
        }
      )
    )
  }
}

case class Schema(name: String, tables: List[Table]) {
  val index = tables.map(t => (t.name.toLowerCase, t)).toMap

  def getTable(name: String): Either[String, Table] = {
    index.get(name.toLowerCase).map(Right.apply _).getOrElse {
      Left(s"""table not found ${this.name}.$name""")
    }
  }

  def getNonAmbiguousColumn(name: String): Either[String, (Table, Column)] = {
    tables.flatMap(t => t.getColumn(name).right.toOption.map(t -> _)) match {
      case Nil => Left(s"column not found $name")
      case col :: Nil => Right(col)
      case _ => Left(s"ambiguous column $name")
    }
  }

}

case class Table(name: String, columns: List[Column]) {
  val index = columns.map(t => (t.name.toLowerCase, t)).toMap

  def getColumn(columnName: String): Either[String, Column] = {
    index.get(columnName.toLowerCase).map {
      column => Right(column)
    }.getOrElse {
      Left(s"""no column $columnName in table $name""")
    }
  }
}

case class Column(name: String, typ: Type)
