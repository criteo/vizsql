package com.criteo.vizatra.vizsql.hive

import com.criteo.vizatra.vizsql._
import com.criteo.vizatra.vizsql.Show._

case class LateralView(inner: Relation, explodeFunction: FunctionCallExpression, tableAlias: String, columnAliases: List[String]) extends Relation {
  def getTables(db: DB) = {
    val result = for {
      innerTables <- inner.getTables(db).right
      placeholders <- inner.getPlaceholders(db).right
    } yield {
      val schemas = Schemas(innerTables.groupBy(_._1).map { case (maybeSchema, tableList) =>
        Schema(maybeSchema.getOrElse(""), tableList.map(_._2))
      }.toList)
      val newDb = db.copy(view = schemas)
      explodeFunction.resultType(newDb, placeholders).right.flatMap {
        case HiveUDTFResult(types) if columnAliases.length == types.length =>
          val columns = columnAliases.zip(types).map { case (alias, typ) =>
            Column(alias, typ)
          }
          Right(innerTables ++ Seq((None, Table(tableAlias, columns))))
        case HiveUDTFResult(types) =>
          Left(ParsingError(s"Expected ${types.size} aliases, got ${columnAliases.size}", pos))
        case _ =>
          Left(ParsingError(s"Expected a UDTF, got ${explodeFunction.show}", pos))
      }
    }
    result.joinRight
  }

  def visit = ???

  def getPlaceholders(db: DB) = inner.getPlaceholders(db)

  def show = ???
}

case class MapOrArrayAccessExpression(map: Expression, key: Expression) extends Expression {
  def getPlaceholders(db: DB, expectedType: Option[Type]) = for {
    mapPlaceholders <- map.getPlaceholders(db, None).right
    keyPlaceholders <- key.getPlaceholders(db, None).right
  } yield mapPlaceholders ++ keyPlaceholders

  def visit = ???

  def resultType(db: DB, placeholders: Placeholders) = (for {
    mapType <- map.resultType(db, placeholders).right
    keyType <- key.resultType(db, placeholders).right
  } yield (mapType, keyType) match {
    case (HiveMap(k, x), _) if k.canBeCastTo(keyType) => Right(x)
    case (HiveMap(k, _), _) => Left(TypeError(s"Expected key type ${keyType.show}, got ${k.show}", pos))
    case (HiveArray(x), INTEGER(_)) => Right(x)
    case (HiveArray(_), x) => Left(TypeError(s"Expected integer index, got ${x.show}", pos))
    case (x, _) => Left(TypeError(s"Expected map or array, got ${x.show}", pos))
  }).joinRight

  def show = map.show ~ "[" ~ key.show ~ "]"
}

case class StructAccessExpr(struct: Expression, field: String) extends Expression {
  def getPlaceholders(db: DB, expectedType: Option[Type]) = struct.getPlaceholders(db, None)

  def visit = ???

  def resultType(db: DB, placeholders: Placeholders) =
    struct.resultType(db, placeholders).right.map {
      case HiveStruct(cols) =>
        cols.find(_.name.toLowerCase == field.toLowerCase).map(_.typ).toRight(SchemaError(s"Field $field not found", pos))
      case HiveArray(HiveStruct(cols)) =>
        cols.find(_.name.toLowerCase == field.toLowerCase).map(c => HiveArray(c.typ)).toRight(SchemaError(s"Field $field not found", pos))
      case x =>
        Left(TypeError(s"Expected struct, got ${x.show}", pos))
    }.joinRight

  def show = struct.show ~ "." ~ field

  override def toColumnName = field
}

case class ColumnOrStructAccessExpression(column: ColumnIdent) extends Expression {
  def getPlaceholders(db: DB, expectedType: Option[Type]) = Right(Placeholders())

  def resultType(db: DB, placeholders: Placeholders) = column match {
    case ColumnIdent(c3, Some(TableIdent(c2, Some(c1)))) =>
      (for {
        schema <- db.view.getSchema(c1).right
        table <- schema.getTable(c2).right
        column <- table.getColumn(c3).right
      } yield column.typ
        ).left.flatMap { _ =>
        val newCol = ColumnOrStructAccessExpression(ColumnIdent(c2, Some(TableIdent(c1, None))))
        newCol.pos = pos
        val structAccess = StructAccessExpr(newCol, c3)
        structAccess.pos = pos
        structAccess.resultType(db, placeholders)
      }
    case ColumnIdent(c2, Some(TableIdent(c1, None))) =>
      (for {
        table <- db.view.getNonAmbiguousTable(c1).right.map(_._2).right
        column <- table.getColumn(c2).right
      } yield column.typ
        ).left.flatMap { _ =>
        val newCol = ColumnOrStructAccessExpression(ColumnIdent(c1, None))
        newCol.pos = pos
        val structAccess = StructAccessExpr(newCol, c2)
        structAccess.pos = pos
        structAccess.resultType(db, placeholders)
      }
    case ColumnIdent(c, None) =>
      (for {
        column <- db.view.getNonAmbiguousColumn(c).right.map(_._3).right
      } yield column.typ
        ).left.map(SchemaError(_, column.pos))
  }

  def show = column.show

  def visit = Nil

  override def toColumnName = column.name
}

case object LeftSemiJoin extends Join {
  def show = keyword("left") ~- keyword("semi") ~- keyword("join")
}
