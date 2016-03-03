package com.criteo.vizatra.vizsql

import java.text.SimpleDateFormat
import java.sql.{Timestamp, Date}

trait Type {
  val nullable: Boolean
  def withNullable(nullable: Boolean): this.type
  def canBeCastTo(other: Type): Boolean
  def show: String
}

case class INTEGER(nullable: Boolean = false) extends Type {
  def withNullable(nullable: Boolean = nullable) = this.copy(nullable).asInstanceOf[this.type]
  def canBeCastTo(other: Type): Boolean = other match {
    case DECIMAL(_) | INTEGER(_) => true
    case _ => false
  }
  def show = "integer"
}
case class DECIMAL(nullable: Boolean = false) extends Type {
  def withNullable(nullable: Boolean = nullable) = this.copy(nullable).asInstanceOf[this.type]
  def canBeCastTo(other: Type): Boolean = other match {
    case DECIMAL(_) => true
    case _ => false
  }
  def show = "decimal"
}
case class BOOLEAN(nullable: Boolean = false) extends Type {
  def withNullable(nullable: Boolean = nullable) = this.copy(nullable).asInstanceOf[this.type]
  def canBeCastTo(other: Type): Boolean = other match {
    case BOOLEAN(_) => true
    case _ => false
  }
  def show = "boolean"
}
case class STRING(nullable: Boolean = false) extends Type {
  def withNullable(nullable: Boolean = nullable) = this.copy(nullable).asInstanceOf[this.type]
  def canBeCastTo(other: Type): Boolean = other match {
    case STRING(_) => true
    case _ => false
  }
  def show = "string"
}
case class TIMESTAMP(nullable: Boolean = false) extends Type {
  def withNullable(nullable: Boolean = nullable) = this.copy(nullable).asInstanceOf[this.type]
  def canBeCastTo(other: Type): Boolean = other match {
    case TIMESTAMP(_) => true
    case _ => false
  }
  def show = "timestamp"
}
case class DATE(nullable: Boolean = false) extends Type {
  def withNullable(nullable: Boolean = nullable) = this.copy(nullable).asInstanceOf[this.type]
  def canBeCastTo(other: Type): Boolean = other match {
    case DATE(_)|TIMESTAMP(_) => true
    case _ => false
  }
  def show = "date"
}

case object NULL extends Type {
  val nullable = true
  def withNullable(nullable: Boolean) = this
  def canBeCastTo(other: Type): Boolean = true
  def show = "null"
}

case class SET(of: Type) extends Type {
  val nullable = of.nullable
  def withNullable(nullable: Boolean = nullable) = SET(of.withNullable(nullable)).asInstanceOf[this.type]
  def canBeCastTo(other: Type): Boolean = other match {
    case SET(x) => of.canBeCastTo(x)
    case _ => false
  }
  def show = s"set(${of.show})"
}

case class RANGE(of: Type) extends Type {
  val nullable = of.nullable
  def withNullable(nullable: Boolean = nullable) = RANGE(of.withNullable(nullable)).asInstanceOf[this.type]
  def canBeCastTo(other: Type): Boolean = other match {
    case RANGE(x) => of.canBeCastTo(x)
    case _ => false
  }
  def show = s"range(${of.show})"
}

object Type {
  case class MissingParameter(err: Err) extends Throwable

  def convertParamList(parameters: Map[String, Any], query : Query) = query.placeholders.right.map(placeholders =>
      parameters.map(x => (x._1 , (placeholders.typeOf(Placeholder(Some(x._1))), x._2))).collect
      {case (x , (Some(y), z)) => (x,(y,z))}.mapValues(x => convertParam(x._1, x._2)))

  def convertParam(pType : Type, value : Any) : Any = pType match {
    case STRING(_) => value match {
      case x :: _ => convertParam(pType, x)
      case s: String => s
      case x => throw new MissingParameter(ParameterError(
        s"unexpected value $x (${x.getClass.getName}) for an SQL STRING parameter", -1
      ))
    }
    case INTEGER(_) => value match {
      case x :: _ => convertParam(pType, x)
      case x: Int => x
      case x: Long => x.toInt
      case x: String => try {
        x.toInt
      } catch {
        case _: Throwable => throw new MissingParameter(ParameterError(
          s"unexpected value $x (${x.getClass.getName}) for an SQL INTEGER parameter", -1
        ))
      }
      case x => throw new MissingParameter(ParameterError(
        s"unexpected value $x (${x.getClass.getName}) for an SQL INTEGER parameter", -1
      ))
    }
    case DATE(_) => value match {
      case x :: _ => convertParam(pType, x)
      case x: String => new Date(new SimpleDateFormat("yyyy-MM-dd").parse(x).getTime)
      case x : java.sql.Date => x
      case x : java.util.Date => new java.sql.Date(x.getTime)
      case x => throw new MissingParameter(ParameterError(
        s"unexpected value $x (${x.getClass.getName}) for an SQL DATE parameter", -1
      ))
    }
    case TIMESTAMP(_) => value match {
      case x :: _ => convertParam(pType, x)
      case x: String => new Timestamp(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(x).getTime)
      case x : java.sql.Timestamp => x
      case x : java.util.Date => new Timestamp(x.getTime)
      case x => throw new MissingParameter(ParameterError(
        s"unexpected value $x (${x.getClass.getName}) for an SQL TIMESTAMP parameter", -1
      ))
    }
    case SET(t) => value match {
      case x: Seq[_] => x.map(x =>convertParam(t, x)).toSet
      case x => Set(convertParam(t, x))
    }
    case RANGE(t) => value match {
      case (a,b) => (convertParam(t, a) ,convertParam(t, b))
      case a :: b :: _ => (convertParam(t, a), convertParam(t, b))
      case x => throw new MissingParameter(ParameterError(
        s"unexpected value $x (${x.getClass.getName}) for an SQL RANGE parameter", -1
      ))
    }
  }
}
