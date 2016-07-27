package com.criteo.vizatra.vizsql

trait SQLFunction {
  def getPlaceholders(call: FunctionCallExpression, db: DB, expectedType: Option[Type]) =
    Utils.allPlaceholders[Expression](call.args, _.getPlaceholders(db, expectedType))
  def resultType(call: FunctionCallExpression, db: DB, placeholders: Placeholders): Either[Err,Type]
  def getArguments(call: FunctionCallExpression, db: DB, placeholders: Placeholders, nb: Int): Either[Err,List[Type]] = {
    if(nb >= 0 && nb > call.args.size) {
      Left(TypeError("expected argument", call.pos + call.name.size + 1))
    }
    else if(nb >= 0 && call.args.size > nb) {
      Left(TypeError("too many arguments", call.args.drop(nb).head.pos))
    }
    else {
      call.args.foldRight(Right(Nil):Either[Err,List[Type]]) {
        (arg, acc) => for(a <- acc.right; b <- arg.resultType(db, placeholders).right) yield b :: a
      }
    }
  }
}

trait SQLFunction0 extends SQLFunction {
  def resultType(call: FunctionCallExpression, db: DB, placeholders: Placeholders) =
    getArguments(call, db, placeholders, 0).right.flatMap { _ =>
      result
    }
  def result: Either[Err,Type]
}

trait SQLFunction1 extends SQLFunction {
  def resultType(call: FunctionCallExpression, db: DB, placeholders: Placeholders) =
    getArguments(call, db, placeholders, 1).right.flatMap { types =>
      result(call.args(0) -> types(0))
    }
  def result: PartialFunction[(Expression,Type),Either[Err,Type]]
}

trait SQLFunction2 extends SQLFunction {
  def resultType(call: FunctionCallExpression, db: DB, placeholders: Placeholders) =
    getArguments(call, db, placeholders, 2).right.flatMap { types =>
      result((call.args(0) -> types(0), call.args(1) -> types(1)))
    }
  def result: PartialFunction[((Expression,Type),(Expression,Type)),Either[Err,Type]]
}

trait SQLFunction3 extends SQLFunction {
  def resultType(call: FunctionCallExpression, db: DB, placeholders: Placeholders) =
    getArguments(call, db, placeholders, 3).right.flatMap { types =>
      result((call.args(0) -> types(0), call.args(1) -> types(1), call.args(2) -> types(2)))
    }
  def result: PartialFunction[((Expression,Type),(Expression,Type),(Expression,Type)),Either[Err,Type]]
}

trait SQLFunctionX extends SQLFunction {
  def resultType(call: FunctionCallExpression, db: DB, placeholders: Placeholders) =
    getArguments(call, db, placeholders, -1).right.flatMap { types =>
      result(call.args.zip(types))
    }
  def result: PartialFunction[List[(Expression,Type)],Either[Err,Type]]
}

object SQLFunction {

  def standard: PartialFunction[String,SQLFunction] = {
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
        case (_, t1) :: tail =>
          tail.foldLeft[Either[Err, Type]](Right(t1)) {
            case (Right(t), (curExpr, curType)) =>
              Utils.parentType(t, curType, TypeError(s"expected ${t.show}, got ${curType.show}", curExpr.pos))
            case (Left(err), _) =>
              Left(err)
          }
      }
    }
    case "count" => new SQLFunctionX {
      override def result = {
        case _ :: _ => Right(INTEGER(false))
      }
    }
  }

}
