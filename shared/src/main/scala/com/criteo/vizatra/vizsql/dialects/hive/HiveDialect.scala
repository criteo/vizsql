package com.criteo.vizatra.vizsql.hive

import com.criteo.vizatra.vizsql._

import scala.util.parsing.input.CharArrayReader.EofCh

case class HiveDialect(udfs: Map[String, SQLFunction]) extends Dialect {

  lazy val parser = new SQL99Parser {
    override val lexical = new SQLLexical {
      override val keywords = SQL99Parser.keywords ++ Set(
        "rlike", "regexp", "limit", "lateral", "view", "distribute", "sort", "cluster", "semi",
        "tablesample", "bucket", "out", "of", "percent")
      override val delimiters = SQL99Parser.delimiters ++ Set("!=", "<=>", "||", "&&", "%", "&", "|", "^", "~", "`", "==")
      override val customToken =
        ( '`'  ~> rep1(chrExcept('`', '\n', EofCh)) <~ '`'   ^^ { x => Identifier(x.mkString) }
        | '\"' ~> rep1(chrExcept('\"', '\n', EofCh)) <~ '\"' ^^ { x => StringLit(x.mkString) }
        | '`'  ~> failure("unclosed backtick")
        | '\'' ~ 'N' ~ 'a' ~ 'N' ~ '\''                      ^^^ DecimalLit("NaN")
        )
    }
    override val comparisonOperators = SQL99Parser.comparisonOperators ++ Set("!=", "<=>", "==")
    override val likeOperators = SQL99Parser.likeOperators ++ Set("rlike", "regexp")
    override val orOperators = SQL99Parser.orOperators + "||"
    override val andOperators = SQL99Parser.andOperators + "&&"
    override val multiplicationOperators = SQL99Parser.multiplicationOperators ++ Set("%", "&", "|", "^")
    override val unaryOperators = SQL99Parser.unaryOperators + "~"
    override val typeMap: Map[String, TypeLiteral] = HiveDialect.typeMap

    lazy val mapOrArrayAccessExpr =
      expr ~ ("[" ~> expr <~ "]") ^^ { case m ~ k => MapOrArrayAccessExpression(m, k) }

    lazy val structAccessExpr =
      expr ~ ("." ~> ident) ^^ { case e ~ f => StructAccessExpr(e, f) }

    override lazy val simpleExpr = (_: Parser[Expression]) =>
      ( literal                ^^ LiteralExpression
      | structAccessExpr
      | mapOrArrayAccessExpr
      | function
      | countStar
      | cast
      | caseWhen
      | column                 ^^ ColumnOrStructAccessExpression
      | "(" ~> select <~ ")"   ^^ SubSelectExpression
      | "(" ~> expr <~ ")"     ^^ ParenthesedExpression
      | expressionPlaceholder
      )

    lazy val tablesampleRelation =
      relation <~ "tablesample" ~ "(" ~ tablesampleExpr ~ ")"

    lazy val tablesampleExpr =
      ( "bucket" ~ integerLiteral ~ "out" ~ "of" ~ integerLiteral ~ opt("on" ~ expr)
      | integerLiteral ~ "percent"
      | integerLiteral ~ elem("bytelength", e => List("b", "B", "k", "K", "m", "g", "G").contains(e.chars))
      )

    lazy val lateralViewRelation =
      relation ~ ("lateral" ~ "view" ~ opt("outer") ~> function) ~ ident ~ ("as" ~> repsep(ident, ",")) ^^ {
        case r ~ f ~ tblAlias ~ colAliases => LateralView(r, f, tblAlias, colAliases)
      }

    override lazy val join =
      ( opt("inner") ~ "join"             ^^^ InnerJoin
      | "left"  ~ opt("outer") ~ "join"   ^^^ LeftJoin
      | "right" ~ opt("outer") ~ "join"   ^^^ RightJoin
      | "full"  ~ opt("outer") ~ "join"   ^^^ FullJoin
      | "cross" ~ "join"                  ^^^ CrossJoin
      | "left"  ~ "semi" ~ "join"         ^^^ LeftSemiJoin
      )

    override lazy val relation: PackratParser[Relation] = pos(
      ( lateralViewRelation
      | tablesampleRelation
      | joinRelation
      | singleTableRelation
      | subSelectRelation

      | failure("table, join or subselect expected")
      )
    )

    lazy val sortBy =
      "sort" ~ "by" ~> repsep(sortExpr, ",")

    lazy val distributeBy =
      "distribute" ~ "by" ~> repsep(expr, ",") ^^ { c => c.map(SortExpression(_, None)) }

    lazy val clusterBy =
      "cluster" ~ "by" ~> repsep(expr, ",") ^^ { c => c.map(SortExpression(_, None)) }

    lazy val xxxBy = // FIXME: losing lots of information by removing the keywords
      ( orderBy ~ clusterBy   ^^ { case o ~ c => o ++ c }
      | distributeBy ~ sortBy ^^ { case d ~ s => d ++ s }
      | orderBy
      | sortBy
      | distributeBy
      | clusterBy
      )

    override lazy val simpleSelect: Parser[SimpleSelect] =
      "select" ~> opt(distinct) ~ rep1sep(projections, ",") ~ opt(relations) ~ opt(filters) ~ opt(groupBy) ~ opt(having) ~ opt(xxxBy) ~ opt(limit) ^^ {
        case d ~ p ~ r ~ f ~ g ~ h ~ o ~ l => SimpleSelect(d, p, r.getOrElse(Nil), f, g.getOrElse(Nil), h, o.getOrElse(Nil), l)
      }
  }

  override def functions = udfs.orElse {
    case "min" | "max" => new SQLFunction1 {
      def result = { case (_, t) => Right(t) }
    }
    case "avg" | "sum" => new SQLFunction1 {
      def result = {
        case (_, t @ (INTEGER(_) | DECIMAL(_))) => Right(t)
        case (arg, _) => Left(TypeError("expected numeric argument", arg.pos))
      }
    }
    case "now" => new SQLFunction0 {
      def result = Right(TIMESTAMP())
    }
    case "concat" => new SQLFunctionX {
      def result = {
        case (_, t1) :: _ => Right(STRING(t1.nullable))
      }
    }
    case "coalesce" => new SQLFunctionX {
      def result = {
        case (_, t1) :: tail => Right(t1)
      }
    }
    case "count" => new SQLFunctionX {
      override def result = {
        case _ :: _ => Right(INTEGER(false))
      }
    }
    case "e" | "pi" =>
      SimpleFunction0(DECIMAL(true))
    case "current_date" =>
      SimpleFunction0(DATE(true))
    case "current_user" =>
      SimpleFunction0(STRING(true))
    case "current_timestamp" =>
      SimpleFunction0(TIMESTAMP(true))
    case "isnull" | "isnotnull" =>
      SimpleFunction1(BOOLEAN(true))
    case "variance" | "var_pop" | "var_samp" | "stddev_pop" | "stddev_samp" | "exp" | "ln" | "log10" |
         "log2" | "sqrt" | "abs" | "sin" | "asin" | "cos" | "acos" | "tan" | "atan" | "degrees" |
         "radians" | "sign" | "cbrt" =>
      SimpleFunction1(DECIMAL(true))
    case "year" | "quarter" | "month" | "day" | "dayofmonth" | "hour" | "minute" | "second" | "weekofyear" |
         "ascii" | "length" | "levenshtein" | "crc32" | "ntile" | "floor" | "ceil" | "ceiling" |
         "factorial" | "shiftleft" | "shiftright" | "shiftrightunsigned" | "size" =>
      SimpleFunction1(INTEGER(true))
    case "to_date" | "last_day" | "base64" | "lower" | "lcase" | "ltrim" | "reverse" | "rtrim" | "space" |
         "trim" | "unbase64" | "upper" | "ucase" | "initcap" | "soundex" | "md5" | "sha" | "sha1" | "bin" |
         "hex" | "unhex" | "binary" =>
      SimpleFunction1(STRING(true))
    case "in_file" | "array_contains" =>
      SimpleFunction2(BOOLEAN(true))
    case "months_between" | "covar_pop" | "covar_samp" | "corr" | "log" | "pow" =>
      SimpleFunction2(DECIMAL(true))
    case "datediff" | "find_in_set" | "instr" =>
      SimpleFunction2(INTEGER(true))
    case "date_add" | "date_sub" | "add_months" | "next_day" | "trunc" | "date_format" | "decode" | "encode" |
         "format_number" | "get_json_object" | "repeat" | "sha2" | "aes_encrypt" | "aes_decrypt" =>
      SimpleFunction2(STRING(true))
    case "from_utc_timestamp" | "to_utc_timestamp" =>
      SimpleFunction2(TIMESTAMP(true))
    case "split" =>
      SimpleFunction2(HiveArray(STRING(true)))
    case "histogram_numeric" =>
      SimpleFunction2(HiveArray(HiveStruct(List(Column("x", DECIMAL(true)), Column("y", DECIMAL(true))))))
    case "round" | "bround" =>
      SimpleFunctionX(1, 2, INTEGER(true))
    case "rand" =>
      SimpleFunctionX(0, 1, INTEGER(true))
    case "lpad" | "regexp_replace" | "rpad" | "translate" | "conv" =>
      SimpleFunctionX(3, 3, STRING(true))
    case "from_unixtime" =>
      SimpleFunctionX(1, 2, STRING(true))
    case "unix_timestamp" =>
      SimpleFunctionX(0, 2, INTEGER(true))
    case "context_ngrams" | "ngrams" =>
      SimpleFunctionX(4, 4, HiveArray(HiveStruct(List(Column("ngram", STRING(true)), Column("estfrequency", DECIMAL(true))))))
    case "concat_ws" | "printf" =>
      SimpleFunctionX(2, None, STRING(true))
    case "locate" =>
      SimpleFunctionX(2, 3, INTEGER(true))
    case "parse_url" | "substr" | "substring" | "regexp_extract" =>
      SimpleFunctionX(2, 3, STRING(true))
    case "sentences" =>
      SimpleFunctionX(1, 3, HiveArray(HiveArray(STRING(true))))
    case "str_to_map" =>
      SimpleFunctionX(1, 3, HiveMap(STRING(true), STRING(true)))
    case "substring_index" =>
      SimpleFunctionX(3, 3, STRING(true))
    case "hash" =>
      SimpleFunctionX(1, None, INTEGER(true))
    case "pmod" =>
      new SQLFunction2 {
        override def result = {
          case ((_, t @ (INTEGER(_) | DECIMAL(_))), _) => Right(t)
        }
      }
    case "positive" | "negative" =>
      new SQLFunction1 {
        override def result = {
          case (_, t @ (INTEGER(_) | DECIMAL(_))) => Right(t)
        }
      }
    case "percentile" =>
      new SQLFunction2 {
        override def result = {
          case (_, (_, a: HiveArray)) => Right(HiveArray(DECIMAL(true)))
          case _ => Right(DECIMAL(true))
        }
      }
    case "percentile_approx" =>
      new SQLFunctionX {
        override def result = {
          case l if l.length >= 2 && l.length <= 3 && l(1)._2.isInstanceOf[HiveArray] => Right(HiveArray(DECIMAL(true)))
          case _ => Right(DECIMAL(true))
        }
      }
    case "collect_set" | "collect_list" =>
      new SQLFunction1 {
        override def result = {
          case (_, t) => Right(HiveArray(t))
        }
      }
    case "if" =>
      new SQLFunctionX {
        override def result = {
          case (_, BOOLEAN(_)) :: (_, t1) :: (_, t2) :: Nil => Right(t1)
          case (arg, _) :: _ => Left(TypeError("Expected boolean argument", arg.pos))
        }
      }
    case "map_keys" =>
      new SQLFunction1 {
        override def result = {
          case (_, HiveMap(k, _)) => Right(HiveArray(k))
        }
      }
    case "map_values" =>
      new SQLFunction1 {
        override def result = {
          case (_, HiveMap(_, v)) => Right(HiveArray(v))
        }
      }
    case "sort_array" =>
      new SQLFunction1 {
        override def result = {
          case (_, a: HiveArray) => Right(a)
        }
      }
    case "map" =>
      new SQLFunctionX {
        override def result = {
          case (_, k) :: (_, v) :: _ => Right(HiveMap(k, v))
        }
      }
    case "struct" =>
      new SQLFunctionX {
        override def result = {
          case l =>
            val cols = l.zipWithIndex.map { case ((_, t), i) => Column(s"col$i", t) }
            Right(HiveStruct(cols))
        }
      }
    case "named_struct" =>
      new SQLFunctionX {
        override def result = {
          case l if l.length % 2 == 0 =>
            val cols = l.grouped(2).toList.map {
              case (LiteralExpression(StringLiteral(name)), _) :: (_, t) :: Nil => Column(name, t)
              case x => sys.error(s"Unsupported expression in named_struct: $x")
            }
            Right(HiveStruct(cols))
        }
      }
    case "array" =>
      new SQLFunctionX {
        override def result = {
          case (_, t) :: _ => Right(HiveArray(t))
        }
      }
    case "explode" =>
      new SQLFunction1 {
        override def result = {
          case (_, HiveArray(elem)) => Right(HiveUDTFResult(List(elem)))
          case (_, HiveMap(key, value)) => Right(HiveUDTFResult(List(key, value)))
        }
      }
    case "inline" =>
      new SQLFunction1 {
        override def result = {
          case (_, HiveArray(HiveStruct(cols))) => Right(HiveUDTFResult(cols.map(_.typ)))
        }
      }
    case "json_tuple" | "parse_url_tuple" =>
      new SQLFunctionX {
        override def result = {
          case l if l.length >= 2 => Right(HiveUDTFResult(l.tail.map(_ => STRING(true))))
        }
      }
    case "posexplode" =>
      new SQLFunction1 {
        override def result = {
          case (_, HiveArray(elem)) => Right(HiveUDTFResult(List(INTEGER(true), elem)))
        }
      }
    case "stack" =>
      new SQLFunctionX {
        override def result = {
          case l if l.length >= 2 => Right(HiveUDTFResult(l.tail.map(_._2)))
        }
      }
    case "java_method" | "reflect" =>
      new SQLFunctionX {
        override def result = {
          case l if l.length >= 2 => Right(STRING(true))
        }
      }
  }
}

object HiveDialect {

  val typeMap = Map( // vizsql types don't matter that much
    "tinyint" -> IntegerTypeLiteral,
    "smallint" -> IntegerTypeLiteral,
    "int" -> IntegerTypeLiteral,
    "bigint" -> IntegerTypeLiteral,
    "float" -> DecimalTypeLiteral,
    "double" -> DecimalTypeLiteral,
    "decimal" -> DecimalTypeLiteral,
    "timestamp" -> TimestampTypeLiteral,
    "string" -> VarcharTypeLiteral,
    "boolean" -> BooleanTypeLiteral,
    "binary" -> VarcharTypeLiteral
  )
}
