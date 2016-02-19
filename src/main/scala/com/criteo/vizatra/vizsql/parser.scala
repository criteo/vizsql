package com.criteo.vizatra.vizsql

import scala.util.parsing.combinator._
import scala.util.parsing.combinator.lexical._
import scala.util.parsing.combinator.syntactical._
import scala.util.parsing.combinator.token._
import scala.util.parsing.input.CharArrayReader.EofCh

trait SQLParser {
  def parseStatement(sql: String): Either[Err,Statement]
}

class SQL99Parser extends SQLParser with TokenParsers with PackratParsers {

  type Tokens = SQLTokens

  trait SQLTokens extends token.Tokens {
    case class Keyword(chars: String) extends Token
    case class Identifier(chars: String) extends Token
    case class IntegerLit(chars: String) extends Token
    case class DecimalLit(chars: String) extends Token
    case class StringLit(chars: String) extends Token
    case class Delimiter(chars: String) extends Token
  }

  object SQL99Lexical {

    val keywords = Set(
      "all", "and", "as", "asc", "between", "boolean", "by", "case", "cast", "count", "cube",
      "date", "datetime", "decimal", "desc", "distinct", "else", "end", "exists", "false", "from", "group", "grouping",
      "in", "inner", "integer", "is", "join", "left", "like",
      "not", "null", "numeric", "on", "or", "order", "outer", "real", "right", "rollup", "select",
      "sets", "then", "timestamp", "true", "unknown", "varchar", "when", "where"
    )

    val delimiters = Set(
      "(", ")", "\"",
      "'", "%", "&",
      "*", "/", "+",
      "-", ",", ".",
      ":", ";", "<",
      ">", "?", "[",
      "]", "_", "|",
      "=", "{", "}",
      "^", "??(", "??)",
      "<>", ">=", "<=",
      "||", "->", "=>"
    )
  }

  class SQLLexical extends Lexical with SQLTokens {

    lazy val whitespace = rep(whitespaceChar)

    val keywords = SQL99Lexical.keywords
    val delimiters = SQL99Lexical.delimiters

    lazy val delimiter = {
      delimiters.toList.sorted.map(s => accept(s.toList) ^^^ Delimiter(s)).foldRight(
        failure("no matching delimiter"): Parser[Token]
      )((x, y) => y | x)
    }

    override lazy val letter = elem("letter", _.isLetter)
    lazy val identifierOrKeyword = letter ~ rep(letter | digit | '_') ^^ {
      case first ~ rest =>
        val chars = first :: rest mkString ""
        if(keywords.contains(chars.toLowerCase)) Keyword(chars.toLowerCase) else Identifier(chars)
    }

    lazy val token =
      ( identifierOrKeyword
      | rep1(digit) ~ ('.' ~> rep1(digit) )             ^^ { case i ~ d => DecimalLit(i.mkString + "." + d.mkString) }
      | rep1(digit)                                     ^^ { case i => IntegerLit(i.mkString) }
      | '\'' ~ rep(chrExcept('\'', '\n', EofCh)) ~ '\'' ^^ { case '\'' ~ chars ~ '\'' => StringLit(chars mkString "") }
      | '\"' ~ rep(chrExcept('\"', '\n', EofCh)) ~ '\"' ^^ { case '\"' ~ chars ~ '\"' => StringLit(chars mkString "") }
      | EofCh                                           ^^^ EOF
      | '\'' ~> failure("unclosed string literal")
      | '\"' ~> failure("unclosed string literal")
      | delimiter
      | failure("illegal character")
      )

  }

  override val lexical = new SQLLexical

  private val tokenParserCache = collection.mutable.HashMap.empty[String,Parser[String]]
  implicit def stringLiteralToKeywordOrDelimiter(chars: String) = {
    if(lexical.keywords.contains(chars) || lexical.delimiters.contains(chars)) {
      tokenParserCache.getOrElseUpdate(
        chars,
        (accept(lexical.Keyword(chars)) | accept(lexical.Delimiter(chars))) ^^ (_.chars) withFailureMessage(s"$chars expected")
      )
    }
    else {
      sys.error(s"""!!! Invalid parser definition: $chars is not a valid SQL keyword or delimiter""")
    }
  }

  // --

  lazy val ident =
    elem("ident", _.isInstanceOf[lexical.Identifier]) ^^ (_.chars)

  lazy val identOrKeyword =
    ( ident
    | elem("keyword", _.isInstanceOf[lexical.Keyword]) ^^ (_.chars)
    )

  lazy val booleanLiteral =
    ( "true"    ^^^ TrueLiteral
    | "false"   ^^^ FalseLiteral
    | "unknown" ^^^ UnknownLiteral
    )

  lazy val nullLiteral =
    "null" ^^^ NullLiteral

  lazy val stringLiteral =
    elem("string", _.isInstanceOf[lexical.StringLit]) ^^ { v => StringLiteral(v.chars) }

  lazy val integerLiteral =
    elem("integer", _.isInstanceOf[lexical.IntegerLit]) ^^ { v => IntegerLiteral(v.chars.toLong) }

  lazy val decimalLiteral =
    elem("decimal", _.isInstanceOf[lexical.DecimalLit]) ^^ { v => DecimalLiteral(v.chars.toDouble) }

  lazy val literal = pos(
    ( decimalLiteral
    | integerLiteral
    | stringLiteral
    | booleanLiteral
    | nullLiteral
    )
  )

  lazy val typeLiteral =
    ( "timestamp" ^^^ TimestampTypeLiteral
    | "datetime"  ^^^ TimestampTypeLiteral
    | "date"      ^^^ DateTypeLiteral
    | "boolean"   ^^^ BooleanTypeLiteral
    | "varchar"   ^^^ VarcharTypeLiteral
    | "integer"   ^^^ IntegerTypeLiteral
    | "numeric"   ^^^ NumericTypeLiteral
    | "decimal"   ^^^ DecimalTypeLiteral
    | "real"      ^^^ RealTypeLiteral
    | failure("type expected")
    )

  lazy val column = pos(
    ( ident ~ "." ~ ident ~ "." ~ ident ^^ { case s ~ _ ~ t ~ _ ~ c => ColumnIdent(c, Some(TableIdent(t, Some(s)))) }
    | ident ~ "." ~ ident               ^^ { case t ~ _ ~ c => ColumnIdent(c, Some(TableIdent(t, None))) }
    | ident                             ^^ { case c => ColumnIdent(c, None) }
    )
  )

  lazy val table =
    ( ident ~ "." ~ ident ^^ { case s ~ _ ~ t => TableIdent(t, Some(s)) }
    | ident               ^^ { case t => TableIdent(t, None) }
    )

  lazy val function =
    identOrKeyword ~ ("(" ~> repsep(expr, ",") <~ ")") ^^ { case n ~ a => FunctionCallExpression(n.toLowerCase, a) }

  lazy val or = (precExpr: Parser[Expression]) =>
    precExpr *
    ( "or" ^^^ OrExpression
    )

  lazy val and = (precExpr: Parser[Expression]) =>
    precExpr *
    ( "and" ^^^ AndExpression
    )

  lazy val not = (precExpr: Parser[Expression]) => {
    def thisExpr: Parser[Expression] =
      ( "not" ~> thisExpr ^^ NotExpression
      | precExpr
      )
    thisExpr
  }

  lazy val exists = (precExpr: Parser[Expression]) =>
    ( "exists" ~> "(" ~> select <~ ")" ^^ ExistsExpression
    | precExpr
    )

  lazy val comparator = (precExpr: Parser[Expression]) =>
    precExpr *
    ( "="     ^^^ IsEqExpression
    | "<>"    ^^^ IsNeqExpression
    | "<"     ^^^ IsLtExpression
    | ">"     ^^^ IsGtExpression
    | ">="    ^^^ IsGeExpression
    | "<="    ^^^ IsLeExpression
    | "like"  ^^^ IsLikeExpression
    )

  lazy val between = (precExpr: Parser[Expression]) =>
    precExpr ~ rep(opt("not") ~ ("between" ~> precExpr ~ ("and" ~> precExpr))) ^^ {
      case l ~ r => r.foldLeft(l) { case (e, n ~ (lb ~ ub)) => IsBetweenExpression(e, n.isDefined, (lb, ub)) }
    }

  lazy val between0 = (precExpr: Parser[Expression]) =>
    precExpr ~ rep(opt("not") ~ ("between" ~> rangePlaceholder)) ^^ {
      case l ~ r => r.foldLeft(l) { case (e, n ~ p) => IsBetweenExpression0(e, n.isDefined, p) }
    }

  lazy val in = (precExpr: Parser[Expression]) =>
    precExpr ~ rep(opt("not") ~ ("in" ~> ("(" ~> rep1sep(expr, ",") <~ ")"))) ^^ {
      case l ~ r => r.foldLeft(l) { case (e, n ~ l) => IsInExpression(e, n.isDefined, l) }
    }

  lazy val in0 = (precExpr: Parser[Expression]) =>
    precExpr ~ rep(opt("not") ~ ("in" ~> setPlaceholder)) ^^ {
      case l ~ r => r.foldLeft(l) { case (e, n ~ p) => IsInExpression0(e, n.isDefined, p) }
    }

  lazy val is = (precExpr: Parser[Expression]) =>
    precExpr ~ rep("is" ~> opt("not") ~ (booleanLiteral | nullLiteral)) ^^ {
      case l ~ r => r.foldLeft(l) { case (e, n ~ l) => IsExpression(e, n.isDefined, l) }
    }

  lazy val add = (precExpr: Parser[Expression]) =>
    precExpr *
    ( "+" ^^^ AddExpression
    | "-" ^^^ SubExpression
    )

  lazy val multiply = (precExpr: Parser[Expression]) =>
    precExpr *
    ( "*" ^^^ MultiplyExpression
    | "/" ^^^ DivideExpression
    )

  lazy val unary = (precExpr: Parser[Expression]) =>
    ( precExpr
    | "-" ~> precExpr ^^ UnaryMinusExpression
    | "+" ~> precExpr ^^ UnaryPlusExpression
    )

  lazy val placeholder =
    opt(ident) ~ opt(":" ~> typeLiteral) ^^ { case i ~ t => ExpressionPlaceholder(Placeholder(i), t) }

  lazy val expressionPlaceholder =
    "?" ~> placeholder

  lazy val rangePlaceholder =
    ("?" ~ "[") ~> placeholder <~ ")"

  lazy val setPlaceholder =
    ("?" ~ "{") ~> placeholder <~ "}"

  lazy val cast =
    ("cast" ~ "(") ~> expr ~ ("as" ~> typeLiteral <~ ")") ^^ { case e ~ t => CastExpression(e, t) }

  lazy val caseWhen =
    ("case" ~> opt(expr) ~ when ~ opt("else" ~> expr) <~ "end" ) ^^ { case maybeValue ~ whenList ~ maybeElse => CaseWhenExpression(maybeValue, whenList, maybeElse) }

  lazy val when: Parser[List[(Expression, Expression)]] =
    rep1("when" ~> expr ~ ("then" ~> expr)) ^^ { _.map { case a ~ b => (a, b) } }

  lazy val simpleExpr = (_: Parser[Expression]) =>
    ( literal                ^^ LiteralExpression
    | function
    | cast
    | caseWhen
    | column                 ^^ ColumnExpression
    | "(" ~> select <~ ")"   ^^ SubSelectExpression
    | "(" ~> expr <~ ")"     ^^ ParenthesedExpression
    | expressionPlaceholder
    )

  lazy val precedenceOrder =
     ( simpleExpr
    :: unary
    :: multiply
    :: add
    :: is
    :: in0
    :: in
    :: between0
    :: between
    :: comparator
    :: exists
    :: not
    :: and
    :: or
    :: Nil
     )

  lazy val expr: PackratParser[Expression] =
    precedenceOrder.reverse.foldRight(failure("Invalid expression"):Parser[Expression])((a,b) => a(pos(b) | failure("expression expected")))

  lazy val starProjection =
    "*" ^^^ AllColumns

  lazy val tableProjection =
    ( (ident ~ "." ~ ident) <~ ("." ~ "*") ^^ { case s ~ _ ~ t => AllTableColumns(TableIdent(t, Some(s))) }
    | ident <~ ("." ~ "*")                 ^^ { case t => AllTableColumns(TableIdent(t, None)) }
    )

  lazy val exprProjection =
    expr ~ opt("as" ~> (ident | stringLiteral)) ^^ {
      case e ~ a => ExpressionProjection(e, a.collect { case StringLiteral(v) => v; case a: String => a })
    }

  lazy val projections = pos(
    ( starProjection
    | tableProjection
    | exprProjection

    | failure("*, table or expression expected")
    )
  )

  lazy val relations =
    "from" ~> rep1sep(relation, ",")

  lazy val relation: PackratParser[Relation] = pos(
    ( joinRelation
    | singleTableRelation
    | subSelectRelation

    | failure("table, join or subselect expected")
    )
  )

  lazy val singleTableRelation =
    table ~ opt(opt("as") ~> (ident | stringLiteral)) ^^ {
      case t ~ a => SingleTableRelation(t, a.collect { case StringLiteral(v) => v; case a: String => a })
    }

  lazy val subSelectRelation =
    ("(" ~> select <~ ")") ~ (opt("as") ~> ident) ^^ {
      case s ~ a => SubSelectRelation(s, a)
    }

  lazy val join =
    ( opt("inner") ~ "join"             ^^^ InnerJoin
    | "left" ~ opt("outer") ~ "join"    ^^^ LeftJoin
    | "right" ~ opt("outer") ~ "join"   ^^^ RightJoin
    )

  lazy val joinRelation =
    relation ~ join ~ relation ~ opt("on" ~> expr) ^^ {
      case l ~ j ~ r ~ o => JoinRelation(l, j, r, o)
    }

  lazy val filters =
    "where" ~> expr

  lazy val groupingSet =
    ( "(" ~ ")"                       ^^^ GroupingSet(Nil)
    | "(" ~> repsep(expr, ",") <~ ")" ^^  GroupingSet
    )

  lazy val groupingSetOrExpr =
    ( groupingSet ^^ Right.apply
    | expr        ^^ Left.apply
    )

  lazy val group =
    ( ("grouping" ~ "sets") ~> ("(" ~> repsep(groupingSet, ",")  <~ ")")  ^^ GroupByGroupingSets
    | "rollup" ~> ("(" ~> repsep(groupingSetOrExpr, ",") <~ ")")          ^^ GroupByRollup
    | "cube" ~> ("(" ~> repsep(groupingSetOrExpr, ",") <~ ")")            ^^ GroupByCube
    | expr                                                                ^^ GroupByExpression
    )

  lazy val groupBy =
    ("group" ~ "by") ~> repsep(group, ",")

  lazy val sortOrder =
    ( "asc"  ^^^ SortASC
    | "desc" ^^^ SortDESC
    )

  lazy val sortExpr =
    expr ~ opt(sortOrder) ^^ { case e ~ o => SortExpression(e, o) }

  lazy val orderBy =
    ("order" ~ "by") ~> repsep(sortExpr, ",")

  lazy val distinct =
    ( "distinct" ^^^ SelectDistinct
    | "all"      ^^^ SelectAll
    )

  lazy val select: Parser[Select] =
    "select" ~> opt(distinct) ~ rep1sep(projections, ",") ~ opt(relations) ~ opt(filters) ~ opt(groupBy) ~ opt(orderBy) ^^ {
      case d ~ p ~ r ~ f ~ g ~ o => Select(d, p, r.getOrElse(Nil), f, g.getOrElse(Nil), o.getOrElse(Nil))
    }

  lazy val statement = pos(
    ( select
    )
  )

  // --

  def pos[T <: SQL](p: => Parser[T]) = Parser { in =>
    p(in) match {
      case Success(t, in1) => Success(t.setPos(in.offset), in1)
      case ns: NoSuccess => ns
    }
  }

  def parseStatement(sql: String): Either[Err,Statement] = {
    phrase(statement <~ opt(";"))(new lexical.Scanner(sql)) match {
      case Success(stmt, _) => Right(stmt)
      case NoSuccess(msg, rest) => Left(ParsingError(msg match {
        case "end of input expected" => "end of statement expected"
        case x => x
      }, rest.offset))
    }
  }

}