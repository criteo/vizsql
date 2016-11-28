package com.criteo.vizatra.vizsql

class Optimizer(db : DB) {

  def optimize(sel : SimpleSelect) : SimpleSelect = {
    val newWhere = sel.where.map(preEvaluate)
    val newProj = sel.projections.map {
      case ExpressionProjection(exp, alias) => ExpressionProjection(preEvaluate(exp), alias)
      case x => x
    }
    val newRel = sel.relations.map(apply)
    val newOrder = sel.orderBy.map {case SortExpression(exp, ord) => SortExpression(preEvaluate(exp), ord) }
    val sel2 = SimpleSelect(sel.distinct, newProj, newRel, newWhere,sel.groupBy, sel.having, newOrder, sel.limit)
    val tables = (sel2.projections.collect {case ExpressionProjection(exp, _) => exp} ++ sel2.where.toList).foldRight(Right(Nil):Either[Err,List[String]]) {
      (p, acc) => for(a <- acc.right; b <- OlapQuery.tablesFor(sel, db, p).right) yield b ++ a
    }.right.getOrElse(Nil)
    SimpleSelect(sel2.distinct, sel2.projections, OlapQuery.rewriteRelations(sel2, db,tables.toSet), sel2.where, sel2.groupBy, sel2.having, sel2.orderBy, sel2.limit)
  }

  def apply(select : Select) : Select = select match {
    case sel : SimpleSelect => optimize(sel)
    case UnionSelect(left, distinct, right) => UnionSelect(this.apply(left), distinct, apply(right))
  }

  def apply(relation: Relation) : Relation  = relation match {
    case JoinRelation(left, join, right, on) => JoinRelation(apply(left), join, apply(right), on.map(preEvaluate))
    case SubSelectRelation(select, alias) => SubSelectRelation(apply(select), alias)
    case x => x
  }

  def preEvaluate(expr : Expression) :  Expression = {
    def convert(v : Either[Literal, Expression]) = v match {case Left(x) => LiteralExpression(x); case Right(x) => x}
    def rec(expr: Expression) : Either[Literal, Expression]  = expr match {
      case LiteralExpression(lit) => Left(lit)
      case ParenthesedExpression(exp) => rec(exp).right.map(x => ParenthesedExpression(x))
      case MathExpression(op, left, right) => (rec(left), rec(right)) match {
        case (Left(x), Left(y)) => EvalHelper.mathEval(op, x, y)
        case (l, r) => Right(MathExpression(op, convert(l), convert(r)))
      }
      case UnaryMathExpression(op, exp) => rec(exp) match {
        case x if op == "+" => x
        case Left(lit) if op == "-" => lit match {
          case DecimalLiteral(x) => Left(DecimalLiteral(-x))
          case IntegerLiteral(x) => Left(IntegerLiteral(-x))
          case _ => Right(UnaryMathExpression(op, LiteralExpression(lit)))
        }
        case x => Right(UnaryMathExpression(op, convert(x)))
      }
      case ComparisonExpression(op, left, right) => (rec(left), rec(right)) match {
        case (Left(x), Left(y)) if EvalHelper.compOperators.contains(op) => EvalHelper.compEval(op, x, y)
        case (l, r) =>  Right(ComparisonExpression(op, convert(l), convert(r)))
      }
      case AndExpression(op, left, right) => (rec(left), rec(right)) match {
        case (Left(x), Left(y)) => Left(EvalHelper.bool2Literal(EvalHelper.literal2bool(x) && EvalHelper.literal2bool(y)))
        case (l, r) =>  Right(AndExpression(op, convert(l), convert(r)))
      }
      case OrExpression(op, left, right) => (rec(left), rec(right)) match {
        case (Left(x), Left(y)) => Left(EvalHelper.bool2Literal(EvalHelper.literal2bool(x) || EvalHelper.literal2bool(y)))
        case (l, r) =>  Right(OrExpression(op, convert(l), convert(r)))
      }
      case IsInExpression(left, not, right) => (rec(left), right.map(rec)) match {
        case (l @ Left(_), r) if not && r.contains(l) => Left(FalseLiteral)
        case (l @ Left(_), r) if r.contains(l) => Left(TrueLiteral)
        case (l, r) => Right(IsInExpression(convert(l), not,
          r.map(convert)))
      }
      case FunctionCallExpression(f, d, args) => Right(FunctionCallExpression(f, d, args.map(x => convert(rec(x)))))
      case NotExpression(exp) => rec(exp) match {
        case Left(l) => Left(EvalHelper.bool2Literal(l == FalseLiteral))
        case Right(ex) => Right(NotExpression(ex))
      }
      case IsExpression(exp, not, lit) => rec(exp) match {
        case Left(lit2) => Left(EvalHelper.bool2Literal(not ^ lit == lit2))
        case Right(ex) => Right(IsExpression(ex, not, lit))
      }
      case CastExpression(from, to) => Right(CastExpression(convert(rec(from)), to))
      case CaseWhenExpression(value, mapping,elseVal) =>
        val valueRes = value.map(rec)
        val elseRes = elseVal.map(rec)
        def recList(l : List[(Either[Literal, Expression],Either[Literal, Expression])]): Either[Literal, Expression] = (valueRes, l, elseRes) match {
          case (Some(matchValue), mapVals@(h::_), elseClause) if matchValue.isRight => Right(CaseWhenExpression(Some(convert(matchValue)),
            mapVals.map(x => (convert(x._1), convert(x._2))), elseClause.map(convert)))
          case (Some(matchValue), h :: t, _) if h._1 == matchValue =>  h._2
          case (Some(matchValue), mapVals @ (h :: t), elseClause) if h._1.isRight =>  Right(CaseWhenExpression(Some(convert(matchValue)),
          mapVals.filter(x => x._1.isRight || x._1 == matchValue).map(x => (convert(x._1), convert(x._2))), elseClause.map(convert)))
          case (None, h::t, _) if h._1 == Left(TrueLiteral) => h._2
          case (None, mapVals @ (h::t), elseClause) if h._1.isRight => Right(CaseWhenExpression(None,
            mapVals.toList.filter(x => x._1.isRight || x._1 == Left(TrueLiteral)).map(x => (convert(x._1), convert(x._2))), elseClause.map(convert)))
          case (matchValue, Nil, Some(elseClause)) => elseClause
          case (matchValue, Nil, None) => Left(NullLiteral)
          case (_, h :: t, _) => recList(t)
        }
        recList(mapping.map(x => (rec(x._1), rec(x._2))))
      case LikeExpression(left, not, op, right) => (rec(left), rec(right)) match {
        case (Left(l), Left(r)) if l == r && !not=> Left(TrueLiteral)
        case (l, r) => Right(LikeExpression(convert(l), not, op, convert(r)))
      }
      case SubSelectExpression(select) => Right(SubSelectExpression(apply(select)))
      case _ => Right(expr)
    }
    convert(rec(expr))
  }
}

object Optimizer {
  def optimize(query : Query) : Query = {
    val sel = new Optimizer(query.db).optimize(query.select)
    Query(sel.toSQL, sel, query.db)
  }
}
