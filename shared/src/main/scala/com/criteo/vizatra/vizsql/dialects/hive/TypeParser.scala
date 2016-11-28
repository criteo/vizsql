package com.criteo.vizatra.vizsql.hive

import com.criteo.vizatra.vizsql._

import scala.util.parsing.combinator.PackratParsers
import scala.util.parsing.combinator.lexical.Lexical
import scala.util.parsing.combinator.syntactical.TokenParsers
import scala.util.parsing.input.CharArrayReader.EofCh

class TypeParser extends TokenParsers with PackratParsers {
  type Tokens = TypeLexical
  override val lexical = new TypeLexical

  class TypeLexical extends Lexical {
    case class ColumnName(chars: String) extends Token
    case class Keyword(chars: String) extends Token
    case class Symbol(chars: String) extends Token

    val keywords = Set(
      "array", "map", "struct", "tinyint", "smallint", "int", "bigint", "integer",
      "double", "float", "decimal", "string", "binary", "boolean", "timestamp"
    )
    val symbols = Set(",", ":", "<", ">")

    lazy val columnNameOrKeyword = rep1(chrExcept('`', ',', ':', '<', '>', EofCh)) ^^ { chrs =>
      val s = chrs.mkString
      if (keywords(s.toLowerCase)) Keyword(s.toLowerCase)
      else ColumnName(s)
    }

    lazy val token =
      ( columnNameOrKeyword
      | (elem(',') | elem(':') | elem('<') | elem('>')) ^^ { c => Symbol(c.toString) }
      | '`' ~> rep1(chrExcept('`', EofCh)) <~ '`'       ^^ { chrs => ColumnName(chrs.mkString) }
      | '`' ~> failure("Unclosed backtick")
      )
    lazy val whitespace = rep(whitespaceChar)
  }

  private val tokenParserCache = collection.mutable.HashMap.empty[String, Parser[String]]
  implicit def string2KeywordOrSymbolParser(chars: String): Parser[String] = tokenParserCache.getOrElseUpdate(
    chars,
    if (lexical.keywords(chars)) accept(lexical.Keyword(chars)) ^^ (_.chars) withFailureMessage s"$chars expected"
    else if (lexical.symbols(chars)) accept(lexical.Symbol(chars)) ^^ (_.chars) withFailureMessage s"$chars expected"
    else sys.error("Invalid parser definition")
  )

  lazy val name = elem("column name", token => token.isInstanceOf[lexical.ColumnName] || token.isInstanceOf[lexical.Keyword]) ^^ (_.chars)

  lazy val array = "array" ~ "<" ~> anyType <~ ">" ^^ { case el => HiveArray(el) }

  lazy val map = ("map" ~ "<" ~> anyType) ~ ("," ~> anyType <~ ">") ^^ { case k ~ v => HiveMap(k, v) }

  lazy val struct = "struct" ~ "<" ~> structFields <~ ">" ^^ { case l =>
    HiveStruct(l.map { case n ~ t => Column(n, t) })
  }

  lazy val structFields = rep1sep(name ~ (":" ~> anyType), ",")

  lazy val simpleType: Parser[Type] =
    ( ("tinyint" | "smallint" | "int" | "bigint" | "integer") ^^^ INTEGER(true)
    | ("double" | "float" | "decimal") ^^^ DECIMAL(true)
    | ("string" | "binary") ^^^ STRING(true)
    | "boolean" ^^^ BOOLEAN(true)
    | "timestamp" ^^^ TIMESTAMP(true)
    | failure("expected type literal")
    )

  lazy val anyType: PackratParser[Type] =
    ( simpleType
    | array
    | struct
    | map
    )

  def parseType(typeName: String): Either[String, Type] =
    phrase(anyType)(new lexical.Scanner(typeName)) match {
      case Success(t, _) => Right(t)
      case NoSuccess(msg, _) => Left(s"$msg for $typeName")
    }

}
