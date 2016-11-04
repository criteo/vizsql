package com.criteo.vizatra.vizsql

import java.util.Date

object EvalHelper {

  def applyOp[T](op: String, x : T, y : T)(implicit num : Numeric[T]) : T = op match {
    case "+" => num.plus(x, y)
    case "-" => num.minus(x, y)
    case "*" => num.times(x, y)
    // Numeric doesn't handle division
  }

  val ops = Set("+", "-", "*")

  def mathEval(op : String, x : Literal, y : Literal) : Either[Literal, Expression] = (x, y) match {
    case (DecimalLiteral(a), DecimalLiteral(b)) if op == "/" && (b != 0) => Left(DecimalLiteral(a / b))
    case (IntegerLiteral(a), DecimalLiteral(b)) if op == "/" && (b != 0) =>  Left(DecimalLiteral(a / b))
    case (DecimalLiteral(a), IntegerLiteral(b)) if op == "/" && (b != 0) => Left(DecimalLiteral(a / b))
    case (IntegerLiteral(a), IntegerLiteral(b)) if op == "/" && (b != 0) && (a % b == 0) => Left(IntegerLiteral(a/b))

    case (DecimalLiteral(a), DecimalLiteral(b)) if ops contains op => Left(DecimalLiteral(applyOp(op, a,b)))
    case (IntegerLiteral(a), DecimalLiteral(b)) if ops contains op => Left(DecimalLiteral(applyOp(op, a,b)))
    case (DecimalLiteral(a), IntegerLiteral(b)) if ops contains op => Left(DecimalLiteral(applyOp(op, a,b)))
    case (IntegerLiteral(a), IntegerLiteral(b)) if ops contains op => Left(IntegerLiteral(applyOp(op, a,b)))
    case (NullLiteral, _) => Left(NullLiteral)
    case (_, NullLiteral) => Left(NullLiteral)
    case _ => Right(MathExpression(op, LiteralExpression(x), LiteralExpression(y)))
  }

  def compEval(op : String, x : Literal, y : Literal) : Either[Literal, Expression] = (x, y) match {
    case (DecimalLiteral(a), DecimalLiteral(b)) => Left(bool2Literal(compOperators(op)(a, b)))
    case (IntegerLiteral(a), DecimalLiteral(b)) => Left(bool2Literal(compOperators(op)(a, b)))
    case (DecimalLiteral(a), IntegerLiteral(b)) => Left(bool2Literal(compOperators(op)(a, b)))
    case (IntegerLiteral(a), IntegerLiteral(b)) => Left(bool2Literal(compOperators(op)(a, b)))
    case (StringLiteral(a), StringLiteral(b)) => Left(bool2Literal(compOperators(op)(a, b)))
    case (NullLiteral, _) => Left(NullLiteral)
    case (_, NullLiteral) => Left(NullLiteral)
    case (l, r) if (l == TrueLiteral || l== FalseLiteral) &&
      (r == TrueLiteral || r == FalseLiteral) => Left(bool2Literal(compOperators(op)(literal2bool(l), literal2bool(r))))
    case _ => Right(ComparisonExpression(op, LiteralExpression(x), LiteralExpression(y)))
  }

  def compare[T<: Any](v1 : T, v2: T) : Int = (v1, v2) match {
    case (x : Ordered[_], y) => x.asInstanceOf[Ordered[Any]] compareTo y.asInstanceOf[Any]
    case (x : Number, y : Number) => x.doubleValue().compare(y.doubleValue())
    case (x : String, y : String) => x.compare(y)
    case (x : Date, y: Date) => x compareTo y
    case (x : Boolean, y : Boolean ) => x compareTo y
    case (x, y) if x == y => 0
  }

  def compOperators = Map(
    ">" -> ((a : Any, b : Any) => compare(a, b) > 0),
    "<" -> ((a : Any, b : Any) => compare(a, b) < 0),
    ">=" -> ((a : Any, b : Any) => compare(a, b) >= 0),
    "<=" -> ((a : Any, b : Any) => compare(a, b) <= 0),
    "=" -> ((a : Any, b : Any) => a == b),
    "<>"-> ((a : Any, b : Any) => a != b)
  )

  def bool2Literal(b : Boolean) = if (b) TrueLiteral else FalseLiteral
  def literal2bool(b: Literal) = (b == TrueLiteral)
}
