package com.criteo.vizatra.vizsql

case class OlapError(val msg: String, val pos: Int) extends Err
case class OlapSelection(parameters: Map[String,Any], filters: Map[String,Any])
case class OlapProjection(dimensions: Set[String], metrics: Set[String])
case class OlapQuery(query: Query) {

  def getProjections: Either[Err,List[ExpressionProjection]] = {
    val validProjections = query.select.projections.collect { case e @ ExpressionProjection(_, Some(_)) => e }
    (query.select.projections diff validProjections) match {
      case Nil => Right(validProjections)
      case oops :: _ => Left(OlapError("Please specify an expression with a label", oops.pos))
    }
  }

  def getParameters: Either[Err,List[String]] = {
    query.placeholders.right.flatMap { placeholders =>
      placeholders.foldRight(Right(Nil):Either[Err,List[String]]) { (p, acc) =>
        acc.right.flatMap { params =>
          p match {
            case (Placeholder(Some(name)), _) => Right(name :: params)
            case (p, _) => Left(OlapError("All parameters must be named", p.pos))
          }
        }
      }
    }
  }

  private def getAllDimensions: Either[Err,(List[String],List[String])] = for {
    columns <- query.columns.right
    projections <- getProjections.right
  } yield projections.collect {
    case ExpressionProjection(e, Some(name)) if query.select.groupBy.flatMap(_.expressions).contains(e) => name
  }.partition { dim =>
    columns.find(_.name == dim).map(_.typ).collect({
      case DATE(_) | TIMESTAMP(_) => true
    }).getOrElse(false)
  }

  def getDimensions: Either[Err,List[String]] = getAllDimensions.right.map(_._2)
  def getTimeDimensions: Either[Err,List[String]] = getAllDimensions.right.map(_._1)

  def getMetrics: Either[Err,List[String]] = for {
    projections <- getProjections.right
  } yield projections.collect {
    case ExpressionProjection(e, Some(name)) if !query.select.groupBy.flatMap(_.expressions).contains(e) => name
  }

  def getProjection(dimensionOrMetric: String): Either[Err,ExpressionProjection] = {
    query.select.projections.collectFirst {
      case e @ ExpressionProjection(_, Some(`dimensionOrMetric`)) => e
    }.toRight(OlapError(s"Not found $dimensionOrMetric", query.select.pos))
  }

  def computeQuery(projection: OlapProjection, selection: OlapSelection): Either[Err,String] = for {
    projectionExpressions <- {
      (if((projection.dimensions ++ projection.metrics).isEmpty) {
        Right[Err,List[ExpressionProjection]]((query.select.projections.collect {
          case e @ ExpressionProjection(_, _) => e
        }))
      } else {
        (projection.dimensions ++ projection.metrics).foldRight(Right(Nil):Either[Err,List[ExpressionProjection]]) {
          (x, acc) => for(a <- acc.right; b <- getProjection(x).right) yield b :: a
        }
      }).right
    }
    select <- OlapQuery.rewriteSelect(query.select, query.db, projectionExpressions, selection.filters.keySet ++ selection.parameters.keySet).right
    sql <- select.fillParameters(query.db, selection.parameters ++ selection.filters).right
    filledQuery <- VizSQL.parseQuery(sql, query.db).right
  } yield {
    Optimizer.optimize(filledQuery).sql
  }

  def rewriteMetricAggregate(metric: String): Either[Err,Option[PostAggregate]] = for {
    expression <- getProjection(metric).right.map(_.expression).right
  } yield {
    def rewriteRecursively(expression: Expression): Option[PostAggregate] = expression match {
      case expr @ FunctionCallExpression("sum", _, _) => Some(SumPostAggregate(expr))
      case FunctionCallExpression("zeroifnull", _, expr :: Nil) => rewriteRecursively(expr)
      case FunctionCallExpression("nullifzero", _, expr :: Nil) => rewriteRecursively(expr)
      case MathExpression("/", left, right) =>
        for {
          leftPA <- rewriteRecursively(left)
          rightPA <- rewriteRecursively(right)
        } yield {
          DividePostAggregate(leftPA, rightPA)
        }
      case _ => None
    }
    rewriteRecursively(expression)
  }

}

object OlapQuery {


  def rewriteSelect(select: Select, db: DB, keepProjections: List[ExpressionProjection], availableParams: Set[String]): Either[Err, Select] = {
    select match {
      case s: SimpleSelect => for {
          projections <- Right(s.projections.filter(x => keepProjections.contains(x))).right
          orderBy <- Right(s.orderBy.filter(f => keepProjections.map(_.expression).contains(f.expression))).right
          where <- Right(rewriteWhereCondition(s, availableParams)).right
          groupBy <- Right(rewriteGroupBy(s, keepProjections.map(_.expression))).right
          tables <- (keepProjections.filter(s.projections.contains).map(_.expression) ++ where.toList).foldRight(Right(Nil):Either[Err,List[String]]) {
            (p, acc) => for(a <- acc.right; b <- tablesFor(s, db, p).right) yield b ++ a
          }.right
          from <- Right(rewriteRelations(s, db, tables.toSet)).right
          rewrittenSelect <- Right(s.copy(
            projections = projections,
            relations = from,
            where = where,
            groupBy = groupBy,
            orderBy = orderBy)).right
          optimizedSelect <- optimizeSubSelect(rewrittenSelect, db, availableParams).right
        } yield {
          optimizedSelect
        }
      case UnionSelect(left, d, right) => for {
        rewrittenLeft <- rewriteSelect(left, db, keepProjections, availableParams).right
        rewrittenRight <- rewriteSelect(right, db, keepProjections, availableParams).right
      } yield {
        UnionSelect(rewrittenLeft, d, rewrittenRight)
      }
    }
  }

  def getTableReferences(select: Select, db: DB, expression: Expression) = {
    select.getQueryView(db).right.flatMap { db =>
      expression.getColumnReferences.foldRight(Right(Nil):Either[Err,List[Table]]) {
        (column, acc) => for {
          a <- acc.right
          b <- (column match {
            case ColumnIdent(_, Some(TableIdent(tableName, Some(schemaName)))) =>
              for {
                schema <- db.view.getSchema(schemaName).right
                table <- schema.getTable(tableName).right
              } yield table
            case ColumnIdent(_, Some(TableIdent(tableName, None))) =>
              db.view.getNonAmbiguousTable(tableName).right.map(_._2)
            case ColumnIdent(column, None) =>
              db.view.getNonAmbiguousColumn(column).right.map(_._2)
          }).left.map(SchemaError(_, column.pos)).right
        } yield b :: a
      }
    }
  }

  def tablesFor(select: Select, db: DB, expression: Expression): Either[Err,List[String]] = {
    getTableReferences(select, db, expression).right.map(_.map(_.name.toLowerCase).distinct)
  }

  def optimizeSubSelect(select: SimpleSelect, db: DB, availableParams: Set[String]): Either[Err,Select] = {
    def optimizeRecursively(relation: Relation): Either[Err,Relation] = relation match {
      case rel @ SubSelectRelation(subSelect, table) =>
        for {
          db <- select.getQueryView(db).right
          optimized <- {
            val allExpressions = select.projections.collect {
              case ExpressionProjection(e, _) => e
            } ++ select.relations.flatMap(r => r :: r.visit).distinct.collect {
              case JoinRelation(_, _, _, Some(e)) => e
            } ++ select.where.toList

            val columnsUsed = allExpressions.flatMap(_.getColumnReferences).collect {
              case ColumnIdent(col, Some(TableIdent(`table`, None))) => col
              case ColumnIdent(col, None) if db.view.getNonAmbiguousColumn(col).right.get._2.name == table => col
            }

            val keepProjections = subSelect.projections.collect {
              case e @ ExpressionProjection(_, Some(alias)) if columnsUsed.contains(alias) => e
            }

            rewriteSelect(subSelect, db, keepProjections, availableParams)
          }.right
        } yield {
          rel.copy(select = optimized)
        }
      case rel @ JoinRelation(left, _, right, _) =>
        for {
          newLeft <- optimizeRecursively(left).right
          newRight <- optimizeRecursively(right).right
        } yield rel.copy(left = newLeft, right = newRight)
      case rel => Right(rel)
    }
    for {
      newRelations <- (select.relations.foldRight(Right(Nil):Either[Err,List[Relation]]) {
        (rel, acc) => for {
          a <- acc.right
          b <- optimizeRecursively(rel).right
        } yield b :: a
      }).right
    } yield select.copy(relations = newRelations)
  }

  def rewriteRelations(select: SimpleSelect, db: DB, tables: Set[String]): List[Relation] = {
    def rewritePass(relations: List[Relation], tables: Set[String]) = {
      def rewriteRecursively(relation: Relation): Option[Relation] = relation match {
        case rel @ SingleTableRelation(_, Some(table)) => if(tables.contains(table.toLowerCase)) Some(rel) else None
        case rel @ SingleTableRelation(TableIdent(table, _), None) => if(tables.contains(table.toLowerCase)) Some(rel) else None
        case rel @ SubSelectRelation(subSelect, table) => if(tables.contains(table.toLowerCase)) Some(rel) else None
        case rel @ JoinRelation(left, _, right, _) =>
          (rewriteRecursively(left) :: rewriteRecursively(right) :: Nil).flatten match {
            case Nil => None
            case x :: Nil => Some(x)
            case l :: r :: Nil => Some(rel.copy(left = l, right = r))
            case _ => sys.error("Unreacheable path")
          }
        case _ => None
      }
      relations.flatMap(rewriteRecursively)
    }

    // We start with the set of tables originally specified,
    // and after each rewrite we check that the rewritten joins
    // don't need more tables.
    def rewriteRecursively(previous: List[Relation], tableSet: Set[String]): List[Relation] = {
      def tablesUsedByRelations(relation: Relation): Set[String] = relation match {
        case JoinRelation(left, _, right, maybeOn) =>
          tablesUsedByRelations(left) ++ tablesUsedByRelations(right) ++ maybeOn.map { on =>
            tablesFor(select, db, on).right.map(_.toSet).right.getOrElse(Set.empty)
          }.getOrElse(Set.empty)
        case _ => Set.empty
      }
      val rewritten = rewritePass(select.relations, tableSet)
      if(rewritten != previous) {
        rewriteRecursively(rewritten, tableSet ++ rewritten.flatMap(tablesUsedByRelations))
      }
      else rewritten
    }

    rewriteRecursively(Nil, tables)
  }

  def rewriteExpression(parameters: Set[String], expression: Expression): Option[Expression] = {
    def rewriteRecursively(expression: Expression): Option[Expression] = expression match {
      case AndExpression(op, left, right) =>
        (rewriteRecursively(left) :: rewriteRecursively(right) :: Nil).flatten match {
          case Nil => None
          case x :: Nil => Some(x)
          case l :: r :: Nil => Some(AndExpression(op, l, r))
          case _ => sys.error("Unreacheable path")
        }
      case expr =>
        (expr.visitPlaceholders.map(_.name).flatten.toSet &~ parameters).toList match {
          case Nil => Some(expr)
          case _ => None
        }
    }
    rewriteRecursively(expression)
  }

  def rewriteWhereCondition(select: SimpleSelect, parameters: Set[String]): Option[Expression] = {
    select.where.flatMap(rewriteExpression(parameters, _))
  }

  def rewriteGroupBy(select: SimpleSelect, expressions: List[Expression]): List[GroupBy] = {
    def rewriteGroupingSet(groupingSet: GroupingSet): Option[GroupingSet] = {
      Option(GroupingSet(groupingSet.groups.filter(expressions.contains))).filterNot(_.groups.isEmpty)
    }
    def rewriteRecursively(groupBy: GroupBy): Option[GroupBy] = groupBy match {
      case g @ GroupByExpression(e) => if(expressions.contains(e)) Some(g) else None
      case g @ GroupByRollup(groups) =>
        Option(GroupByRollup(groups.map {
          case g @ Left(e) => if(expressions.contains(e)) Some(g) else None
          case g @ Right(gs) => rewriteGroupingSet(gs).map(Right.apply)
        }.flatten)).filterNot(_.groups.isEmpty)
    }
    select.groupBy.flatMap(rewriteRecursively)
  }

}

//

trait PostAggregate
case class SumPostAggregate(expr: Expression) extends PostAggregate
case class DividePostAggregate(left: PostAggregate, right: PostAggregate) extends PostAggregate
