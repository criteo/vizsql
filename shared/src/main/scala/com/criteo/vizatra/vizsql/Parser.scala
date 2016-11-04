package com.criteo.vizatra.vizsql

import scala.util.parsing.combinator._
import scala.util.parsing.combinator.lexical._
import scala.util.parsing.combinator.syntactical._
import scala.util.parsing.input.CharArrayReader.EofCh

trait SQLParser {
  def parseStatement(sql: String): Either[Err,Statement]
}

object SQL99Parser {
  val keywords = Set(
    "all", "and", "as", "asc", "between", "by", "case", "cast", "cross", "cube",
    "desc", "distinct", "else", "end", "exists", "false", "from", "full", "group", "grouping",
    "having", "in", "inner", "is", "join", "left", "like", "limit",
    "not", "null", "on", "or", "order", "outer", "right", "rollup", "select",
    "sets", "then", "true", "union", "unknown", "when", "where"
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
  val comparisonOperators = Set("=", "<>", "<", ">", ">=", "<=")

  val likeOperators = Set("like")

  val orOperators = Set("or")

  val andOperators = Set("and")

  val additionOperators = Set("+", "-")

  val multiplicationOperators = Set("*", "/")

  val unaryOperators = Set("-", "+")

  val typeMap: Map[String, TypeLiteral] = Map(
    "timestamp" -> TimestampTypeLiteral,
    "datetime"  -> TimestampTypeLiteral,
    "date"      -> DateTypeLiteral,
    "boolean"   -> BooleanTypeLiteral,
    "varchar"   -> VarcharTypeLiteral,
    "integer"   -> IntegerTypeLiteral,
    "numeric"   -> NumericTypeLiteral,
    "decimal"   -> DecimalTypeLiteral,
    "real"      -> RealTypeLiteral
  )
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

  class SQLLexical extends Lexical with SQLTokens {

    lazy val whitespace = rep(whitespaceChar | blockComment | lineComment)

    val keywords = SQL99Parser.keywords
    val delimiters = SQL99Parser.delimiters

    lazy val blockComment = '/' ~ '*' ~ rep(chrExcept('*', EofCh) | ('*' ~ chrExcept('/', EofCh))) ~ '*' ~ '/'
    lazy val lineComment = '-' ~ '-' ~ rep(chrExcept('\n', '\r', EofCh))

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

    def customToken: Parser[Token] = elem("should not exist", _ => false) ^^ { _ => sys.error("custom token should not exist")}

    lazy val exp = (accept('e') | accept('E')) ~ opt('-') ~ rep1(digit) ^^ { case e ~ n ~ d => e.toString + n.mkString + d.mkString}

    def quoted(delimiter: Char) =
      delimiter ~> rep(
        ( (delimiter ~ delimiter) ^^^ delimiter
        | chrExcept(delimiter, '\n', EofCh)
        )
      ) <~ delimiter

    lazy val token =
      ( customToken
      | identifierOrKeyword
      | rep1(digit) ~ ('.' ~> rep(digit)) ~ opt(exp)    ^^ { case i ~ d ~ mE => DecimalLit(i.mkString + "." + d.mkString + mE.mkString) }
      | rep1(digit) ~ exp                               ^^ { case i ~ e => DecimalLit(i.mkString + e) }
      | '.' ~> rep1(digit) ~ opt(exp)                   ^^ { case d ~ mE => DecimalLit("." + d.mkString + mE.mkString) }
      | rep1(digit)                                     ^^ { case i => IntegerLit(i.mkString) }
      | quoted('\'')                                    ^^ { case chars => StringLit(chars mkString "") }
      | quoted('\"')                                    ^^ { case chars => Identifier(chars mkString "") }
      | EofCh                                           ^^^ EOF
      | '\'' ~> failure("unclosed string literal")
      | '\"' ~> failure("unclosed identifier")
      | delimiter
      | failure("illegal character")
      )

  }

  override val lexical = new SQLLexical

  implicit def stringLiteralToKeywordOrDelimiter(chars: String) = {
    if(lexical.keywords.contains(chars) || lexical.delimiters.contains(chars)) {
      (accept(lexical.Keyword(chars)) | accept(lexical.Delimiter(chars))) ^^ (_.chars) withFailureMessage(s"$chars expected")
    }
    else {
      sys.error(s"""!!! Invalid parser definition: $chars is not a valid SQL keyword or delimiter""")
    }
  }

  // --

  lazy val ident =
    elem("ident", _.isInstanceOf[lexical.Identifier]) ^^ (_.chars)

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

  lazy val typeLiteral: Parser[TypeLiteral] =
    elem("type literal", { t => typeMap.contains(t.chars.toLowerCase) }) ^^ { t => typeMap(t.chars.toLowerCase) }

  val typeMap = SQL99Parser.typeMap

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
    ident ~ ("(" ~> opt(distinct) ~ repsep(expr, ",") <~ ")") ^^ { case n ~ (d ~ a) => FunctionCallExpression(n.toLowerCase, d, a) }

  lazy val countStar =
    elem("count", _.chars.toLowerCase == "count") ~ "(" ~ "*" ~ ")" ^^^ CountStarExpression

  lazy val or = (precExpr: Parser[Expression]) =>
    precExpr *
    orOperators.map { op => op ^^^ OrExpression.operator(op) _ }.reduce(_ | _)

  val orOperators = SQL99Parser.orOperators

  lazy val and = (precExpr: Parser[Expression]) =>
    precExpr *
    andOperators.map { op => op ^^^ AndExpression.operator(op) _ }.reduce(_ | _)

  val andOperators = SQL99Parser.andOperators

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
    comparisonOperators.map { op => op ^^^ ComparisonExpression.operator(op)_ }.reduce(_ | _)

  val comparisonOperators = SQL99Parser.comparisonOperators

  lazy val like = (precExpr: Parser[Expression]) =>
    precExpr *
    likeOperators.map { op =>
      opt("not") <~ op ^^ { case o => LikeExpression.operator(op, o.isDefined)_ }
    }.reduce(_ | _)

  val likeOperators = SQL99Parser.likeOperators

  lazy val limit = "limit" ~> integerLiteral

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
    additionOperators.map { op => op ^^^ MathExpression.operator(op)_ }.reduce(_ | _)

  val additionOperators = SQL99Parser.additionOperators

  lazy val multiply = (precExpr: Parser[Expression]) =>
    precExpr *
    multiplicationOperators.map { op => op ^^^ MathExpression.operator(op)_ }.reduce(_ |  _)

  val multiplicationOperators = SQL99Parser.multiplicationOperators

  lazy val unary = (precExpr: Parser[Expression]) =>
    ((unaryOperators.map { op =>
      op ~ precExpr ^^ { case `op` ~ p => UnaryMathExpression(op, p) }
    }: Set[Parser[Expression]]) + precExpr).reduce(_ | _)

  val unaryOperators = SQL99Parser.unaryOperators

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
    | countStar
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
    :: like
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
    expr ~ opt(opt("as") ~> (ident | stringLiteral)) ^^ {
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
    | "left"  ~ opt("outer") ~ "join"   ^^^ LeftJoin
    | "right" ~ opt("outer") ~ "join"   ^^^ RightJoin
    | "full"  ~ opt("outer") ~ "join"   ^^^ FullJoin
    | "cross" ~ "join"                  ^^^ CrossJoin
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

  lazy val having =
    "having" ~> expr

  lazy val sortOrder =
    ( "asc"  ^^^ SortASC
    | "desc" ^^^ SortDESC
    )

  lazy val sortExpr =
    expr ~ opt(sortOrder) ^^ { case e ~ o => SortExpression(e, o) }

  lazy val orderBy =
    ("order" ~ "by") ~> repsep(sortExpr, ",")

  lazy val distinct =
    ( "distinct" ^^^ SetDistinct
    | "all"      ^^^ SetAll
    )

  lazy val unionSelect: Parser[UnionSelect] =
    select ~ ("union" ~> opt(distinct)) ~ select ^^ {
      case l ~ d ~ r => UnionSelect(l, d, r)
    }

  lazy val simpleSelect: Parser[SimpleSelect] =
    "select" ~> opt(distinct) ~ rep1sep(projections, ",") ~ opt(relations) ~ opt(filters) ~ opt(groupBy) ~ opt(having) ~ opt(orderBy) ~ opt(limit) ^^ {
      case d ~ p ~ r ~ f ~ g ~ h ~ o ~ l => SimpleSelect(d, p, r.getOrElse(Nil), f, g.getOrElse(Nil), h, o.getOrElse(Nil), l)
    }

  lazy val select: PackratParser[Select] = pos(
    ( unionSelect
    | simpleSelect
    )
  )

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
