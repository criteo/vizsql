package com.criteo.vizatra.vizsql

import Utils._
import Show._

trait SQL {
  var pos: Int = -1
  def setPos(newpos: Int): SQL.this.type = {
    pos = newpos
    this
  }
  def show: Show
  def toSQL(implicit style: Style) = show.toSQL(style)
}

// ----- Literals

trait Literal extends SQL {
  val mapType: Type
}

case class IntegerLiteral(value: Long) extends Literal {
  val mapType = INTEGER()
  def show = value.toString
}

case class DecimalLiteral(value: Double) extends Literal {
  val mapType = DECIMAL()
  def show = value.toString
}

case class StringLiteral(value: String) extends Literal {
  val mapType = STRING()
  def show = s"""'${value.toString.replace("'", "''")}'"""
}

case object NullLiteral extends Literal {
  val mapType = NULL
  def show = keyword("null")
}

case object TrueLiteral extends Literal {
  val mapType = BOOLEAN()
  def show = keyword("true")
}

case object FalseLiteral extends Literal {
  val mapType = BOOLEAN()
  def show = keyword("false")
}

case object UnknownLiteral extends Literal {
  val mapType = BOOLEAN()
  def show = keyword("unknown")
}

// ----- Type literal

trait TypeLiteral extends SQL {
  val mapType: Type
}

case object IntegerTypeLiteral extends TypeLiteral {
  val mapType = INTEGER()
  def show = keyword("integer")
}

case object VarcharTypeLiteral extends TypeLiteral {
  val mapType = STRING()
  def show = keyword("varchar")
}

case object NumericTypeLiteral extends TypeLiteral {
  val mapType = DECIMAL()
  def show = keyword("numeric")
}

case object RealTypeLiteral extends TypeLiteral {
  val mapType = DECIMAL()
  def show = keyword("real")
}

case object DecimalTypeLiteral extends TypeLiteral {
  val mapType = DECIMAL()
  def show = keyword("decimal")
}

case object TimestampTypeLiteral extends TypeLiteral {
  val mapType = TIMESTAMP()
  def show = keyword("timestamp")
}

case object DateTypeLiteral extends TypeLiteral {
  val mapType = DATE()
  def show = keyword("date")
}

case object BooleanTypeLiteral extends TypeLiteral {
  val mapType = BOOLEAN()
  def show = keyword("boolean")
}

// ----- Columns

case class TableIdent(name: String, schema: Option[String] = None) extends SQL {
  def show = schema match {
    case Some(s) => ident(s) ~ "." ~ ident(name)
    case _ => ident(name)
  }
}
case class ColumnIdent(name: String, table: Option[TableIdent] = None) extends SQL {
  def show = table match {
    case Some(t) => t.show ~ "." ~ ident(name)
    case _ => ident(name)
  }
}

// ------ Expressions

trait Expression extends SQL {
  def getPlaceholders(db: DB, expectedType: Option[Type]): Either[Err,Placeholders]
  def resultType(db: DB, placeholders: Placeholders): Either[Err,Type]
  def toColumnName = show.toSQL(implicitly[Style])
  def visit: List[Expression]
  def visitPlaceholders: List[Placeholder] = visit.flatMap(_.visitPlaceholders)
  def getColumnReferences: List[ColumnIdent] = {
    def collectColumns0(exprs: List[Expression]): List[ColumnIdent] = {
      exprs.flatMap {
        case ColumnExpression(c) => List(c)
        case x => collectColumns0(x.visit)
      }
    }
    collectColumns0(this :: Nil)
  }
}

case class ParenthesedExpression(expression: Expression) extends Expression {
  def getPlaceholders(db: DB, expectedType: Option[Type]) = expression.getPlaceholders(db, expectedType)
  def resultType(db: DB, placeholders: Placeholders) = expression.resultType(db, placeholders)
  def show = "(" ~ expression.show ~ ")"
  def visit = expression :: Nil
}

case class LiteralExpression(literal: Literal) extends Expression {
  def getPlaceholders(db: DB, expectedType: Option[Type]) = Right(Placeholders())
  def resultType(db: DB, placeholders: Placeholders) = Right(literal.mapType)
  def show = literal.show
  def visit = Nil
}

case class ColumnExpression(column: ColumnIdent) extends Expression {
  def getPlaceholders(db: DB, expectedType: Option[Type]) = Right(Placeholders())
  def resultType(db: DB, placeholders: Placeholders) = column match {
    case ColumnIdent(columnName, Some(TableIdent(tableName, Some(schemaName)))) =>
      (for {
        schema <- db.view.getSchema(schemaName).right
        table <- schema.getTable(tableName).right
        column <- table.getColumn(columnName).right
      } yield column.typ
      ).left.map(SchemaError(_, column.pos))
    case ColumnIdent(columnName, Some(TableIdent(tableName, None))) =>
      (for {
        table <- db.view.getNonAmbiguousTable(tableName).right.map(_._2).right
        column <- table.getColumn(columnName).right
      } yield column.typ
      ).left.map(SchemaError(_, column.pos))
    case ColumnIdent(columnName, None) =>
      (for {
        column <- db.view.getNonAmbiguousColumn(columnName).right.map(_._3).right
      } yield column.typ
      ).left.map(SchemaError(_, column.pos))
  }
  def show = column.show
  def visit = Nil
}

trait MathExpression extends Expression {
  val left: Expression
  val right: Expression

  def getPlaceholders(db: DB, expectedType: Option[Type]) = for {
    leftPlaceholders <- left.getPlaceholders(db, Some(DECIMAL())).right
    rightPlaceholders <- right.getPlaceholders(db, Some(DECIMAL())).right
  } yield {
    leftPlaceholders ++ rightPlaceholders
  }

  def resultType(db: DB, placeholders: Placeholders): Either[Err, Type] = for {
    leftType <- left.resultType(db, placeholders).right
    rightType <- right.resultType(db, placeholders).right
  } yield {
    val isNullable = leftType.nullable || rightType.nullable
    (leftType, rightType) match {
      case (TIMESTAMP(_), DECIMAL(_) | INTEGER(_)) => TIMESTAMP(nullable = isNullable)
      case (DECIMAL(_) | INTEGER(_), TIMESTAMP(_)) => TIMESTAMP(nullable = isNullable)
      case _ => DECIMAL(nullable = isNullable)
    }
  }

  def visit = left :: right :: Nil
}


case class AddExpression(left: Expression, right: Expression) extends MathExpression {
  def show = left.show ~- "+" ~- right.show
}

case class SubExpression(left: Expression, right: Expression) extends MathExpression {
  def show = left.show ~- "-" ~- right.show
}

case class MultiplyExpression(left: Expression, right: Expression) extends MathExpression {
  def show = left.show ~- "*" ~- right.show
}

case class DivideExpression(left: Expression, right: Expression) extends MathExpression {
  def show = left.show ~- "/" ~- right.show
}

trait ComparisonExpression extends Expression {
  val left: Expression
  val right: Expression
  def getPlaceholders(db: DB, expectedType: Option[Type]) = {
    firstKnownType(left :: right :: Nil, db, None).right.flatMap { tl =>
      for {
        leftPlaceholders <- left.getPlaceholders(db, Some(tl)).right
        rightPlaceholders <- right.getPlaceholders(db, Some(tl)).right
      } yield {
        leftPlaceholders ++ rightPlaceholders
      }
    }
  }
  def resultType(db: DB, placeholders: Placeholders) = for {
    leftType <- left.resultType(db, placeholders).right
    rightType <- right.resultType(db, placeholders).right
  } yield {
    BOOLEAN(nullable = leftType.nullable || rightType.nullable)
  }
  def visit = left :: right :: Nil
}

case class IsEqExpression(left: Expression, right: Expression) extends ComparisonExpression {
  def show = left.show ~- "=" ~- right.show
}

case class IsNeqExpression(left: Expression, right: Expression) extends ComparisonExpression {
  def show = left.show ~- "<>" ~- right.show
}

case class IsLtExpression(left: Expression, right: Expression) extends ComparisonExpression {
  def show = left.show ~- "<" ~- right.show
}

case class IsGtExpression(left: Expression, right: Expression) extends ComparisonExpression {
  def show = left.show ~- ">" ~- right.show
}

case class IsGeExpression(left: Expression, right: Expression) extends ComparisonExpression {
  def show = left.show ~- ">=" ~- right.show
}

case class IsLeExpression(left: Expression, right: Expression) extends ComparisonExpression {
  def show = left.show ~- "<=" ~- right.show
}

case class IsLikeExpression(left: Expression, right: Expression) extends ComparisonExpression {
  def show = left.show ~- keyword("like") ~- right.show
}

trait BooleanExpression extends Expression {
  val left: Expression
  val right: Expression
  def getPlaceholders(db: DB, expectedType: Option[Type]) = for {
    leftPlaceholders <- left.getPlaceholders(db, Some(BOOLEAN())).right
    rightPlaceholders <- right.getPlaceholders(db, Some(BOOLEAN())).right
  } yield {
    leftPlaceholders ++ rightPlaceholders
  }
  def resultType(db: DB, placeholders: Placeholders) = for {
    leftType <- left.resultType(db, placeholders).right
    rightType <- right.resultType(db, placeholders).right
  } yield {
    BOOLEAN(nullable = leftType.nullable || rightType.nullable)
  }
  def visit = left :: right :: Nil
}

case class AndExpression(left: Expression, right: Expression) extends BooleanExpression {
  def show = left.show ~/ keyword("and") ~- right.show
}

case class OrExpression(left: Expression, right: Expression) extends BooleanExpression {
  def show = left.show ~/ keyword("or") ~- right.show
}

case class IsInExpression(left: Expression, not: Boolean, right: List[Expression]) extends Expression {
  def getPlaceholders(db: DB, expectedType: Option[Type]) = {
    firstKnownType(left :: right, db, None).right.flatMap { tl =>
      for {
        leftPlaceholders <- left.getPlaceholders(db, Some(tl)).right
        rightPlaceholders <- right.foldRight(Right(Placeholders()):Either[Err,Placeholders]) {
          (p, acc) => for(a <- acc.right; b <- p.getPlaceholders(db, Some(tl)).right) yield b ++ a
        }.right
      } yield {
        leftPlaceholders ++ rightPlaceholders
      }
    }
  }
  def resultType(db: DB, placeholders: Placeholders) = {
    left.resultType(db, placeholders).right.map(t => BOOLEAN(nullable = t.nullable))
  }
  def show = left.show ~- (if(not) Some(keyword("not")) else None) ~- keyword("in") ~- "(" ~ join(right.map(_.show), ", ") ~ ")"
  def visit = left :: right
}

case class IsInExpression0(left: Expression, not: Boolean, inExpr: ExpressionPlaceholder) extends Expression {
  def getPlaceholders(db: DB, expectedType: Option[Type]) = {
    for {
      leftPlaceholders <- left.getPlaceholders(db, None).right
      leftType <- left.resultType(db, leftPlaceholders).right
    } yield {
      leftPlaceholders + (inExpr.placeholder -> (inExpr.explicitType.map(t => SET(t.mapType)).getOrElse(SET(leftType))))
    }
  }
  def resultType(db: DB, placeholders: Placeholders) = {
    left.resultType(db, placeholders).right.map(t => BOOLEAN(nullable = t.nullable))
  }
  def show = left.show ~- (if(not) Some(keyword("not")) else None) ~- keyword("in") ~- inExpr.show
  def visit = left :: Nil
  override def visitPlaceholders = inExpr.placeholder :: Nil
}

case class FunctionCallExpression(name: String, args: List[Expression]) extends Expression {
  def getPlaceholders(db: DB, expectedType: Option[Type]) = {
    db.function(name)
      .left.map(s => TypeError(s, this.pos))
      .right.flatMap(_.getPlaceholders(this, db, expectedType))
  }
  def resultType(db: DB, placeholders: Placeholders) = {
    db.function(name)
      .left.map(s => TypeError(s, this.pos))
      .right.flatMap(_.resultType(this, db, placeholders))
  }
  def show = keyword(name) ~ "(" ~ join(args.map(_.show), ", ") ~ ")"
  def visit = args
}

case class IsBetweenExpression(expression: Expression, not: Boolean, bounds: (Expression, Expression)) extends Expression {
  def getPlaceholders(db: DB, expectedType: Option[Type]) = {
    firstKnownType(expression :: bounds._1 :: bounds._2 :: Nil, db, None).right.flatMap { tl =>
      for {
        exprPlaceholders <- expression.getPlaceholders(db, Some(tl)).right
        lowerBoundPlaceholders <- bounds._1.getPlaceholders(db, Some(tl)).right
        uperBoundPlaceholders <- bounds._2.getPlaceholders(db, Some(tl)).right
      } yield {
        exprPlaceholders ++ lowerBoundPlaceholders ++ uperBoundPlaceholders
      }
    }
  }
  def resultType(db: DB, placeholders: Placeholders) = for {
    exprType <- expression.resultType(db, placeholders).right
    lBoundType <- bounds._1.resultType(db, placeholders).right
    uBoundType <- bounds._2.resultType(db, placeholders).right
  } yield {
    BOOLEAN(nullable = exprType.nullable || lBoundType.nullable || uBoundType.nullable)
  }
  def show = expression.show ~- (if(not) Some(keyword("not")) else None) ~- keyword("between") ~- bounds._1.show ~- keyword("and") ~- bounds._2.show
  def visit = expression :: bounds._1 :: bounds._2 :: Nil
}

case class IsBetweenExpression0(expression: Expression, not: Boolean, boundsExpr: ExpressionPlaceholder) extends Expression {
  def getPlaceholders(db: DB, expectedType: Option[Type]) = {
    for {
      leftPlaceholders <- expression.getPlaceholders(db, None).right
      leftType <- expression.resultType(db, leftPlaceholders).right
    } yield {
      leftPlaceholders + (boundsExpr.placeholder -> (boundsExpr.explicitType.map(t => RANGE(t.mapType)).getOrElse(RANGE(leftType))))
    }
  }
  def resultType(db: DB, placeholders: Placeholders) = for {
    exprType <- expression.resultType(db, placeholders).right
  } yield {
    BOOLEAN(nullable = exprType.nullable )
  }
  def show = expression.show ~- (if(not) Some(keyword("not")) else None) ~- keyword("between") ~- boundsExpr.show
  def visit = expression :: Nil
  override def visitPlaceholders = boundsExpr.placeholder :: Nil
}

case class NotExpression(expression: Expression) extends Expression {
  def getPlaceholders(db: DB, expectedType: Option[Type]) = expression.getPlaceholders(db, Some(BOOLEAN()))
  def resultType(db: DB, placeholders: Placeholders) = for {
    exprType <- expression.resultType(db, placeholders).right
  } yield {
    BOOLEAN(nullable = exprType.nullable)
  }
  def show = keyword("not") ~- expression.show
  def visit = expression :: Nil
}

case class IsExpression(expression: Expression, not: Boolean, literal: Literal) extends Expression {
  def getPlaceholders(db: DB, expectedType: Option[Type]) = ???
  def resultType(db: DB, placeholders: Placeholders) = for {
    exprType <- expression.resultType(db, placeholders).right
  } yield {
    BOOLEAN(nullable = exprType.nullable)
  }
  def show = expression.show ~- keyword("is") ~- (if(not) Some(keyword("not")) else None) ~- literal.show
  def visit = expression :: Nil
}

case class ExistsExpression(select: Select) extends Expression {
  def getPlaceholders(db: DB, expectedType: Option[Type]) = ???
  def resultType(db: DB, placeholders: Placeholders) = Right(BOOLEAN(nullable = false))
  def show = keyword("exists") ~- "(" ~| (select.show) ~ ")"
  def visit = ???
}

case class UnaryMinusExpression(expression: Expression) extends Expression {
  def getPlaceholders(db: DB, expectedType: Option[Type]) = expression.getPlaceholders(db, Some(DECIMAL()))
  def resultType(db: DB, placeholders: Placeholders) = expression.resultType(db, placeholders)
  def show = "-" ~ expression.show
  def visit = expression :: Nil
}

case class UnaryPlusExpression(expression: Expression) extends Expression {
  def getPlaceholders(db: DB, expectedType: Option[Type]) = expression.getPlaceholders(db, Some(DECIMAL()))
  def resultType(db: DB, placeholders: Placeholders) = expression.resultType(db, placeholders)
  def show = "+" ~ expression.show
  def visit = expression :: Nil
}

case class SubSelectExpression(select: Select) extends Expression {
  def getPlaceholders(db: DB, expectedType: Option[Type]) = ???
  def resultType(db: DB, placeholders: Placeholders) = ???
  def show = "(" ~| (select.show) ~ ")"
  def visit = ???
}

case class CastExpression(from: Expression, to: TypeLiteral) extends Expression {
  def getPlaceholders(db: DB, expectedType: Option[Type]) = from.getPlaceholders(db, None)
  def resultType(db: DB, placeholders: Placeholders) = for {
    fromType <- from.resultType(db, placeholders).right
  } yield {
    to.mapType.withNullable(fromType.nullable)
  }
  def show = keyword("cast") ~ "(" ~ from.show ~- keyword("as") ~- to.show ~ ")"
  def visit = from :: Nil
}

case class CaseWhenExpression(value: Option[Expression], mapping: List[(Expression, Expression)], elseVal: Option[Expression]) extends Expression {

  def getPlaceholders(db: DB, expectedType: Option[Type]) =
      for {
        valPlaceholders <- value.map(_.getPlaceholders(db, None)).getOrElse(Right(Placeholders())).right
        valType <- value.map(_.resultType(db, valPlaceholders)).getOrElse(Right(BOOLEAN(true))).right
        resultType <- firstKnownType(mapping.map(_._2) ++ elseVal, db, expectedType).right
        mappingPlaceholders <- mapping.foldLeft(Right(Placeholders()): Either[Err, Placeholders]) { case (acc, (cond, res)) =>
          for {
            a <- acc.right
            condPlaceholders <- cond.getPlaceholders(db, Some(valType)).right
            resPlaceholders <- res.getPlaceholders(db, Some(resultType)).right
          } yield a ++ condPlaceholders ++ resPlaceholders
        }.right
        elsePlaceHolders <- elseVal.map(_.getPlaceholders(db, Some(resultType))).getOrElse(Right(Placeholders())).right
      } yield valPlaceholders ++ mappingPlaceholders ++ elsePlaceHolders
  def resultType(db: DB, placeholders: Placeholders) =
    firstKnownType(mapping.map(_._2) ++ elseVal, db, None).right.map { t =>
      t.withNullable(t.nullable || elseVal.isEmpty)
    }
  def show =
    keyword("case") ~- value.map(_.show) ~| (
      mapping.map { case (cond, res) =>
        keyword("when") ~- cond.show ~- keyword("then") ~- res.show
      }.reduceLeft(_ ~/ _) ~/ elseVal.map(keyword("else") ~- _.show)
    ) ~/ keyword("end")
  def visit = value.toList ++ mapping.flatMap { case (cond, res) => Seq(cond, res)} ++ elseVal
}

// ------ Placeholders

case class Placeholders(items: List[(Placeholder,Type)] = Nil) extends Seq[(Placeholder,Type)]{
  def +(item: (Placeholder,Type)) = Placeholders(items :+ item)
  def ++(o: Placeholders) = Placeholders(items ++ o.items)
  def typeOf(o: Placeholder) = items.find(_._1 == o).map(_._2)
  def types = items.map(_._2)
  def namesAndTypes = items.map(x => (x._1.name, x._2))
  def iterator = items.iterator
  def apply(idx: Int) = items(idx)
  def length = items.length
  override def toString = s"""Placeholders(${items.map(_.toString).mkString(",")})"""
}

case class Placeholder(name: Option[String]) extends SQL {
  def show = ~?(this)
}

case class ExpressionPlaceholder(placeholder: Placeholder, explicitType: Option[TypeLiteral]) extends Expression {
  def getPlaceholders(db: DB, expectedType: Option[Type]) = {
    explicitType.map(_.mapType).orElse(expectedType) match {
      case Some(t) => Right(Placeholders() + (placeholder -> t))
      case None => Left(PlaceholderError(s"parameter type cannot be inferred from context", this.pos))
    }
  }
  def resultType(db: DB, placeholders: Placeholders) = placeholders.typeOf(placeholder).map {
    case t => Right(t)
  }.getOrElse(Left(PlaceholderError("a parameter was not expected here", this.pos)))
  def show = placeholder.show
  def visit = Nil
  override def visitPlaceholders = placeholder :: Nil
}

// ----- Projections

trait Projection extends SQL {
  def getColumns(db: DB): Either[Err,List[Column]]
  def getPlaceholders(db: DB): Either[Err,Placeholders]
}

case object AllColumns extends Projection {
  def getColumns(db: DB) = Right(db.view.schemas.flatMap(_.tables).flatMap(_.columns))
  def getPlaceholders(db: DB) = Right(Placeholders())
  def show = "*"
}

case class AllTableColumns(table: TableIdent) extends Projection {
  def getColumns(db: DB) = table match {
    case TableIdent(tableName, Some(schemaName)) =>
      (for {
        schema <- db.view.getSchema(schemaName).right
        table <- schema.getTable(tableName).right
      } yield table.columns
      ).left.map(SchemaError(_, table.pos))
    case TableIdent(tableName, None) =>
      (for {
        table <- db.view.getNonAmbiguousTable(tableName).right.map(_._2).right
      } yield table.columns
      ).left.map(SchemaError(_, table.pos))
  }
  def getPlaceholders(db: DB) = Right(Placeholders())
  def show = table.show ~ ".*"
}

case class ExpressionProjection(expression: Expression, alias: Option[String] = None) extends Projection {
  def getColumns(db: DB) = for {
    placeholders <- getPlaceholders(db).right
    resultType <- expression.resultType(db, placeholders).right
  } yield {
     Column(alias.getOrElse(expression.toColumnName), resultType) :: Nil
  }
  def getPlaceholders(db: DB) = expression.getPlaceholders(db, None)
  def show = expression.show ~- (alias.map(a => keyword("as") ~- a))
}

// ----- Relations

trait Relation extends SQL{
  def getTables(db: DB): Either[Err,List[(Option[String],Table)]]
  def getPlaceholders(db: DB): Either[Err,Placeholders]
  def visit: List[Relation]
}

case class SingleTableRelation(table: TableIdent, alias: Option[String] = None) extends Relation {
  def getTables(db: DB) = table match {
    case TableIdent(tableName, Some(schemaName)) =>
      (for {
        schema <- db.schemas.getSchema(schemaName).right
        table <- schema.getTable(tableName).right
      } yield {
        alias.map { newName =>
          (None, table.copy(name = newName)) :: Nil
        }.getOrElse {
          (Some(schema.name), table) :: Nil
        }
      }
      ).left.map(SchemaError(_, table.pos))
    case TableIdent(tableName, None) =>
      (for {
        schemaAndTable <- db.schemas.getNonAmbiguousTable(tableName).right
      } yield {
        alias.map { newName =>
          (None, schemaAndTable._2.copy(name = newName)) :: Nil
        }.getOrElse {
          (Some(schemaAndTable._1.name), schemaAndTable._2) :: Nil
        }
      }
      ).left.map(SchemaError(_, table.pos))
  }
  def getPlaceholders(db: DB) = Right(Placeholders())
  def show = table.show ~- (alias.map(a => keyword("as") ~- a))
  def visit = Nil
}

case class SubSelectRelation(select: Select, alias: String) extends Relation {
  def getTables(db: DB) = select.getColumns(db).right.map { columns =>
    (None, Table(alias, columns)) :: Nil
  }
  def getPlaceholders(db: DB) = select.getPlaceholders(db)
  def show = "(" ~| (select.show) ~ ")" ~- keyword("as") ~- alias
  def visit = Nil
}

case class JoinRelation(left: Relation, join: Join, right: Relation, on: Option[Expression] = None) extends Relation {
  def getTables(db: DB) = for {
    leftTables <- left.getTables(db).right
    rightTables <- right.getTables(db).right
  } yield {
    leftTables ++ rightTables
  }
  def getPlaceholders(db: DB) = for {
    leftPlaceholders <- left.getPlaceholders(db).right
    rightPlaceholders <- right.getPlaceholders(db).right
    onPlaceholders <- on.map(_.getPlaceholders(db, Some(BOOLEAN()))).getOrElse(Right(Placeholders())).right
  } yield (leftPlaceholders ++ rightPlaceholders ++ onPlaceholders)
  def show = left.show ~/ join.show ~- right.show ~| on.map(e => keyword("on") ~- e.show)
  def visit = left :: right :: Nil
}

// ----- Joins

trait Join extends SQL
case object InnerJoin extends Join { def show = keyword("join") }
case object LeftJoin extends Join { def show = keyword("left") ~- keyword("join") }
case object RightJoin extends Join { def show = keyword("right") ~- keyword("join") }

// ----- Group by

trait GroupBy extends SQL {
  def expressions: List[Expression]
}

case class GroupByExpression(expression: Expression) extends GroupBy {
  def show = expression.show
  def expressions = expression :: Nil
}

case class GroupingSet(groups: List[Expression]) extends SQL {
  def show = "(" ~| join(groups.map(_.show), "," ~/ "" ) ~ ")"
  def expressions = groups
}

case class GroupByGroupingSets(groups: List[GroupingSet]) extends GroupBy {
  def show = keyword("grouping") ~- keyword("sets") ~ ("(" ~| join(groups.map(_.show), "," ~/ "") ~ ")")
  def expressions = groups.flatMap(_.expressions)
}

case class GroupByRollup(groups: List[Either[Expression,GroupingSet]]) extends GroupBy {
  def show = keyword("rollup") ~ ("(" ~| join(groups.map(_.fold(_.show, _.show)), "," ~/ "") ~ ")")
  def expressions = groups.flatMap(_.fold(_ :: Nil, _.expressions))
}

case class GroupByCube(groups: List[Either[Expression,GroupingSet]]) extends GroupBy {
  def show = keyword("cube") ~ ("(" ~| join(groups.map(_.fold(_.show, _.show)), "," ~/ "") ~ ")")
  def expressions = groups.flatMap(_.fold(_ :: Nil, _.expressions))
}

// ----- Statements

trait Statement extends SQL {
  def getPlaceholders(db: DB): Either[Err,Placeholders]
  def fillParameters(
    db: DB,
    namedPameters: Map[String,Any] = Map.empty,
    anonymousParameters: List[Any] = Nil
  )(implicit style: Style): Either[Err,String] = for {
    placeholders <- getPlaceholders(db).right
    sql <- show.toSQL(style, placeholders, namedPameters, anonymousParameters).right
  } yield sql
}

trait DistinctClause extends SQL
case object SelectAll extends DistinctClause { def show = keyword("all") }
case object SelectDistinct extends DistinctClause { def show = keyword("distinct") }

trait SortOrder extends SQL
case object SortASC extends SortOrder { def show = keyword("asc") }
case object SortDESC extends SortOrder { def show = keyword("desc") }

case class SortExpression(expression: Expression, order: Option[SortOrder]) extends SQL {
  def show = expression.show ~- order.map(_.show)
}

case class Select(
  distinct: Option[DistinctClause] = None,
  projections: List[Projection] = Nil,
  relations: List[Relation] = Nil,
  where: Option[Expression] = None,
  groupBy: List[GroupBy] = Nil,
  orderBy: List[SortExpression] = Nil) extends Statement {

  def getTables(db: DB) = {
    relations.foldRight(Right(Nil):Either[Err,List[(Option[String],Table)]]) {
      (r, acc) => for(a <- acc.right; b <- r.getTables(db).right) yield b ++ a
    }
  }

  def getQueryView(db: DB) = getTables(db).right.map { tables =>
    db.copy(view = Schemas(
      tables.groupBy(_._1).map {
        case (schemaName, tables) => Schema(schemaName.getOrElse(""), tables.map(_._2))
      }.toList
    ))
  }

  def getColumns(db: DB) = for {
    db <- getQueryView(db).right
    columns <- projections.foldRight(Right(Nil):Either[Err,List[Column]]) {
      (p, acc) => for(a <- acc.right; b <- p.getColumns(db).right) yield b ++ a
    }.right
  } yield columns

  def getPlaceholders(db: DB) = {
    val oo = Right(Placeholders()):Either[Err,Placeholders]
    for {
      db <- getQueryView(db).right
      projectionsPlaceholders <- projections.foldRight(oo) {
        (p, acc) => for(a <- acc.right; b <- p.getPlaceholders(db).right) yield b ++ a
      }.right
      relationsPlaceholders <- relations.foldRight(oo) {
        (r, acc) => for(a <- acc.right; b <- r.getPlaceholders(db).right) yield b ++ a
      }.right
      wherePlaceholders <- where.map(_.getPlaceholders(db, Some(BOOLEAN()))).getOrElse(oo).right
    } yield {
      projectionsPlaceholders ++ relationsPlaceholders ++ wherePlaceholders
    }
  }

  def show =
    keyword("select") ~- distinct.map(_.show) ~|
      (join(projections.map(_.show), "," ~/ "")) ~
    Option(relations).filterNot(_.isEmpty).map { relations =>
      keyword("from") ~|
        (join(relations.map(_.show), "," ~/ ""))
    } ~
    where.map { e =>
      keyword("where") ~| e.show
    } ~
    Option(groupBy).filterNot(_.isEmpty).map { relations =>
      keyword("group") ~- keyword("by") ~|
        (join(groupBy.map(_.show), "," ~/ ""))
    } ~
    Option(orderBy).filterNot(_.isEmpty).map { expressions =>
      keyword("order") ~- keyword("by") ~|
        (join(expressions.map(_.show), "," ~/ ""))
    }
}

object Utils {

  def extractAllTypes(exprs: List[Expression], db: DB, expectedType: Option[Type]): List[Either[Err, Type]] = {
    exprs.map { expr =>
      for {
        exprPlaceholders <- expr.getPlaceholders(db, expectedType).right
        exprType <- expr.resultType(db, exprPlaceholders).right
      } yield exprType
    }
  }

  def firstKnownType(exprs: List[Expression], db: DB, expectedType: Option[Type]): Either[Err,Type] = {
    expectedType.map(Right.apply).getOrElse {
      val all = extractAllTypes(exprs, db, None)
      all.find(_.isRight).getOrElse {
        Left(all.collect { case Left(err) => err }.foldLeft[Err](NoError) { case (e1, e2) => e1.combine(e2) })
      }
    }.right.map { expectedType =>
      val all = extractAllTypes(exprs, db, Some(expectedType))
      val allTypes = all.collect { case Right(t) => t }
      if (allTypes.isEmpty)
        Left(all.collect { case Left(err) => err }.foldLeft[Err](NoError) { case (e1, e2) => e1.combine(e2) })
      else
        allTypes.zipWithIndex.find { case (t, i) =>
          !t.canBeCastTo(expectedType)
        }.map { case (t, i) =>
          Left(TypeError(s"expected ${expectedType.show}, found ${t.show}", exprs(i).pos))
        }.getOrElse(Right(expectedType.withNullable(expectedType.nullable || allTypes.exists(_.nullable))))
    }.joinRight
  }

}
