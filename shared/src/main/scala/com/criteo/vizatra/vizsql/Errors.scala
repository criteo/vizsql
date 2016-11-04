package com.criteo.vizatra.vizsql

trait Err {
  val msg: String
  val pos: Int

  def toString(sql: String, lineChar: Char = '~') = {
    val carretPosition = sql.size - sql.drop(pos).dropWhile(_.toString.matches("""\s""")).size

    sql.split('\n').foldLeft("") {
      case (sql, line) if sql.size <= carretPosition && (sql.size + line.size) >= carretPosition => {
        val carret = (1 to (carretPosition - sql.size)).map(_ => lineChar).mkString + "^"
        s"$sql\n$line\n$carret\nError: $msg\n"
      }
      case (sql, line) => s"$sql\n$line"
    }.split('\n').dropWhile(_.isEmpty).reverse.dropWhile(_.isEmpty).reverse.mkString("\n")
  }

  def combine(err: Err): Err = this
}

case class PlaceholderError(msg: String, pos: Int) extends Err {
  override def combine(err: Err) = err match {
    case SchemaError(_, _) | TypeError(_, _) => err
    case _ => this
  }
}
case class ParsingError(msg: String, pos: Int) extends Err
case class SchemaError(msg: String, pos: Int) extends Err
case class TypeError(msg: String, pos: Int) extends Err
case class SQLError(msg: String, pos: Int) extends Err
case class ParameterError(msg: String, pos: Int) extends Err
case object NoError extends Err {
  val msg = "?WAT"
  val pos = 0
  override def combine(err: Err) = err
}