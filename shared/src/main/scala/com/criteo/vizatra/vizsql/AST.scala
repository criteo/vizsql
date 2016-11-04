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

case class MathExpression(op: String, left: Expression, right: Expression) extends Expression {
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
  def show = left.show ~- keyword(op) ~- right.show
}

object MathExpression {
  def operator(op: String)(left: Expression, right: Expression) = apply(op, left, right)
}

case class UnaryMathExpression(op: String, expression: Expression) extends Expression {
  def getPlaceholders(db: DB, expectedType: Option[Type]) = expression.getPlaceholders(db, Some(DECIMAL()))
  def resultType(db: DB, placeholders: Placeholders) = expression.resultType(db, placeholders)
  def show = keyword(op) ~ expression.show
  def visit = expression :: Nil
}

case class ComparisonExpression(op: String, left: Expression, right: Expression) extends Expression {
  def getPlaceholders(db: DB, expectedType: Option[Type]) = {
    commonParentType(left :: right :: Nil, db, None).right.flatMap { tl =>
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
  def show = left.show ~- keyword(op) ~- right.show
}

object ComparisonExpression {
  def operator(op: String)(left: Expression, right: Expression) = apply(op, left, right)
}

case class LikeExpression(left: Expression, not: Boolean, op: String, right: Expression) extends Expression {
  def getPlaceholders(db: DB, expectedType: Option[Type]) = {
    commonParentType(left :: right :: Nil, db, None).right.flatMap { tl =>
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
  def show = left.show ~- (if(not) Some(keyword("not")) else None) ~- keyword(op) ~- right.show
}

object LikeExpression {
  def operator(op: String, not: Boolean)(left: Expression, right: Expression) = apply(left, not, op, right)
}

trait BooleanExpression extends Expression {
  def op: String
  def right: Expression
  def left: Expression
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
  def show = left.show ~/ keyword(op) ~- right.show
}

case class AndExpression(op: String, left: Expression, right: Expression) extends BooleanExpression

object AndExpression {
  def operator(op: String)(left: Expression, right: Expression) = apply(op, left, right)
}

case class OrExpression(op: String, left: Expression, right: Expression) extends BooleanExpression

object OrExpression {
  def operator(op: String)(left: Expression, right: Expression) = apply(op, left, right)
}

case class IsInExpression(left: Expression, not: Boolean, right: List[Expression]) extends Expression {
  def getPlaceholders(db: DB, expectedType: Option[Type]) = {
    commonParentType(left :: right, db, None).right.flatMap { tl =>
      for {
        leftPlaceholders <- left.getPlaceholders(db, Some(tl)).right
        rightPlaceholders <- allPlaceholders[Expression](right, _.getPlaceholders(db, Some(tl))).right
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

case class FunctionCallExpression(name: String, distinct: Option[SetSpec], args: List[Expression]) extends Expression {
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
  def show = {
    val argsShow = join(args.map(_.show), ", ")
    keyword(name) ~ "(" ~ distinct.map(_.show ~- argsShow).getOrElse(argsShow) ~ ")"
  }
  def visit = args
}

case object CountStarExpression extends Expression {
  def getPlaceholders(db: DB, expectedType: Option[Type]) = Right(Placeholders())
  def visit = Nil
  def resultType(db: DB, placeholders: Placeholders) = Right(INTEGER(false))
  def show = keyword("count") ~ "(" ~ "*" ~ ")"
}

case class IsBetweenExpression(expression: Expression, not: Boolean, bounds: (Expression, Expression)) extends Expression {
  def getPlaceholders(db: DB, expectedType: Option[Type]) = {
    commonParentType(expression :: bounds._1 :: bounds._2 :: Nil, db, None).right.flatMap { tl =>
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
  def getPlaceholders(db: DB, expectedType: Option[Type]) = literal match {
    case NullLiteral => expression.getPlaceholders(db, None)
    case l => expression.getPlaceholders(db, Some(l.mapType))
  }
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

case class SubSelectExpression(select: Select) extends Expression {
  def getPlaceholders(db: DB, expectedType: Option[Type]) = select.getPlaceholders(db)
  def resultType(db: DB, placeholders: Placeholders) = ???
  def show = "(" ~| (select.show) ~ ")"
  def visit = Nil
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

  def getPlaceholders(db: DB, expectedType: Option[Type]) = {
    val conditions = mapping.map(_._1) ++ value
    val results = mapping.map(_._2) ++ elseVal
    val expectedConditionType = if (value.isEmpty) Some(BOOLEAN(true)) else None
    for {
      cType <- commonParentType(conditions, db, expectedConditionType).right
      cPlaceholders <- allPlaceholders[Expression](conditions, _.getPlaceholders(db, Some(cType))).right
      rType <- commonParentType(results, db, expectedType).right
      rPlaceholders <-  allPlaceholders[Expression](results, _.getPlaceholders(db, Some(rType))).right
    } yield cPlaceholders ++ rPlaceholders
  }
  def resultType(db: DB, placeholders: Placeholders) =
    commonParentType(mapping.map(_._2) ++ elseVal, db, None).right.map { t =>
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
case object FullJoin extends Join { def show = keyword("full") ~- keyword("join") }
case object CrossJoin extends Join { def show = keyword("cross") ~- keyword("join") }

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

// ----- Other clauses

trait SetSpec extends SQL
case object SetAll extends SetSpec { def show = keyword("all") }
case object SetDistinct extends SetSpec { def show = keyword("distinct") }

trait SortOrder extends SQL
case object SortASC extends SortOrder { def show = keyword("asc") }
case object SortDESC extends SortOrder { def show = keyword("desc") }

case class SortExpression(expression: Expression, order: Option[SortOrder]) extends SQL {
  def show = expression.show ~- order.map(_.show)
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

trait Select extends Statement {
  def projections: List[Projection]
  def getTables(db: DB): Either[Err,List[(Option[String], Table)]]
  def getQueryView(db: DB) = getTables(db).right.map { tables =>
    db.copy(view = Schemas(
      tables.groupBy(_._1).map {
        case (schemaName, tables) => Schema(schemaName.getOrElse(""), tables.map(_._2))
      }.toList
    ))
  }
  def getColumns(db: DB): Either[Err,List[Column]]
}

case class UnionSelect(left: Select, distinct: Option[SetSpec], right: Select) extends Select {
  lazy val projections = left.projections ++ right.projections

  def getTables(db: DB) = for {
    leftTables <- left.getTables(db).right
    rightTables <- right.getTables(db).right
  } yield leftTables ++ rightTables

  def getColumns(db: DB) = (for {
    leftCols <- left.getColumns(db).right
    rightCols <- right.getColumns(db).right
  } yield {
    if (leftCols.length != rightCols.length) {
      Left(TypeError("expected same number of columns on both sides of the union", right.pos))
    } else {
      val zipped = leftCols.zip(rightCols)
      zipped.foldRight[Either[Err,List[Column]]](Right(List.empty)) {
        case ((l, r), Right(acc)) =>
          parentType(l.typ, r.typ, TypeError(s"expected ${l.typ.show}, found ${r.typ.show} for column ${r.name}", right.pos)) match {
            case Right(typ) => Right(l.copy(typ = typ) :: acc)
            case Left(e) => Left(e)
          }
        case (_, e@Left(_)) => e
      }
    }
  }).joinRight

  def getPlaceholders(db: DB) = for {
    leftPlaceholders <- left.getPlaceholders(db).right
    rightPlaceholders <- right.getPlaceholders(db).right
  } yield leftPlaceholders ++ rightPlaceholders

  def show = left.show ~/ keyword("union") ~- distinct.map(_.show) ~/ right.show
}

case class SimpleSelect(
  distinct: Option[SetSpec] = None,
  projections: List[Projection] = Nil,
  relations: List[Relation] = Nil,
  where: Option[Expression] = None,
  groupBy: List[GroupBy] = Nil,
  having: Option[Expression] = None,
  orderBy: List[SortExpression] = Nil,
  limit: Option[IntegerLiteral] = None) extends Select {

  def getTables(db: DB) = {
    relations.foldRight(Right(Nil):Either[Err,List[(Option[String],Table)]]) {
      (r, acc) => for(a <- acc.right; b <- r.getTables(db).right) yield b ++ a
    }
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
      projectionsPlaceholders <- allPlaceholders[Projection](projections, _.getPlaceholders(db)).right
      relationsPlaceholders <- allPlaceholders[Relation](relations, _.getPlaceholders(db)).right
      wherePlaceholders <- where.map(_.getPlaceholders(db, Some(BOOLEAN()))).getOrElse(oo).right
      havingPlaceholders <- having.map(_.getPlaceholders(db, Some(BOOLEAN()))).getOrElse(oo).right
    } yield {
      projectionsPlaceholders ++ relationsPlaceholders ++ wherePlaceholders ++ havingPlaceholders
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
    having.map { e =>
      keyword("having") ~| e.show
    } ~
    Option(orderBy).filterNot(_.isEmpty).map { expressions =>
      keyword("order") ~- keyword("by") ~|
        (join(expressions.map(_.show), "," ~/ ""))
    } ~
    limit.map { e =>
      keyword("limit") ~- e.show
    }
}

object Utils {

  def allPlaceholders[A](l: List[A], getPlaceholders: A => Either[Err,Placeholders]) =
    l.foldRight(Right(Placeholders()):Either[Err,Placeholders]) {
      (el, acc) => for(a <- acc.right; b <- getPlaceholders(el).right) yield b ++ a
    }

  def parentType(a: Type, b: Type, err: => Err): Either[Err, Type] =
    if (a.canBeCastTo(b)) Right(b)
    else if (b.canBeCastTo(a)) Right(a)
    else Left(err)

  def extractAllTypes(exprs: List[Expression], db: DB, expectedType: Option[Type]): List[Either[Err, Type]] = {
    exprs.map { expr =>
      for {
        exprPlaceholders <- expr.getPlaceholders(db, expectedType).right
        exprType <- expr.resultType(db, exprPlaceholders).right
      } yield exprType
    }
  }

  def commonParentType(exprs: List[Expression], db: DB, expectedType: Option[Type]): Either[Err,Type] = {
    expectedType.map(Right.apply).getOrElse {
      val all = extractAllTypes(exprs, db, None)
      if (all.exists(_.isRight)) {
        all.zipWithIndex.collect { case (Right(t), i) => (t, i) }.foldLeft[Either[Err, Type]](Right(NULL)) {
          case (Right(acc), (cur, i)) =>
            parentType(acc, cur, TypeError(s"expected ${acc.show}, found ${cur.show}", exprs(i).pos))
          case (err@Left(_), _) => err
        }
      } else {
        Left(all.collect { case Left(err) => err }.foldLeft[Err](NoError) { case (e1, e2) => e1.combine(e2) })
      }
    }.right.flatMap { expectedType =>
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
    }
  }

}
