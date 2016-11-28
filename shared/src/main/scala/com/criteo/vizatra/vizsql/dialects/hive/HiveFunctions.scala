package com.criteo.vizatra.vizsql.hive

import com.criteo.vizatra.vizsql.Show._
import com.criteo.vizatra.vizsql._

case class SimpleFunction0(resultType: Type) extends SQLFunction0 {
  override def result = Right(resultType)
}

case class SimpleFunction1(resultType: Type) extends SQLFunction1 {
  override def result = {
    case _ => Right(resultType)
  }
}

case class SimpleFunction2(resultType: Type) extends SQLFunction2 {
  override def result = {
    case _ => Right(resultType)
  }
}

case class SimpleFunctionX(minArgs: Int, maxArgs: Option[Int], resultType: Type) extends SQLFunctionX {
  override def result = {
    case l if l.length >= minArgs && maxArgs.forall(l.length <= _) =>
      Right(resultType)
    case l =>
      Left(ParsingError(s"wrong argument list size ${l.length}", l.headOption.fold(0)(_._1.pos)))
  }
}

object SimpleFunctionX {
  def apply(minArgs: Int, maxArgs: Int, resultType: Type): SimpleFunctionX = apply(minArgs, Some(maxArgs), resultType)
}
