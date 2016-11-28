package com.criteo.vizatra.vizsql

object VizSQL {

  def parseQuery(sql: String, db: DB): Either[Err,Query] =
    (db.dialect.parser).parseStatement(sql).right.flatMap {
      case select @ SimpleSelect(_, _, _, _, _, _, _, _) => Right(Query(sql, select, db))
      case stmt => Left(SQLError("select expected", stmt.pos))
    }

  def parseOlapQuery(sql: String, db: DB): Either[Err,OlapQuery] = (for {
    query <- parseQuery(sql, db).right
  } yield OlapQuery(query)).right.flatMap { q =>
    q.query.error.map(err => Left(err)).getOrElse(Right(q))
  }

}

case class Query(sql: String, select: SimpleSelect, db: DB) {

  def tables = select.getTables(db)
  def columns = select.getColumns(db)
  def placeholders = select.getPlaceholders(db)
  def queryView = select.getQueryView(db)

  def error = (for {
    _ <- tables.right
    _ <- columns.right
    _ <- placeholders.right
  } yield ()).fold(Some.apply _, _ => None)
}