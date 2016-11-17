package com.criteo.vizatra.vizsql

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

  def convertParamList(parameters: Map[String, Any], query : Query) = query.placeholders.right.map { placeholders =>
    for {
      (name, value) <- parameters
      typ <- placeholders.typeOf(Placeholder(Some(name)))
    } yield {
      name -> convertParam(typ, value)
    }
  }

  def convertParam(pType : Type, value : Any) : FilledParameter = pType match {
    case STRING(_) => value match {
      case x :: _ => convertParam(pType, x)
      case s: String => StringParameter(s)
      case n: Number => StringParameter(n.toString)
      case b: Boolean => StringParameter(b.toString)
      case c: Char => StringParameter(c.toString)
      case x => throw new MissingParameter(ParameterError(
        s"unexpected value $x (${x.getClass.getName}) for an SQL STRING parameter", -1
      ))
    }
    case INTEGER(_) => value match {
      case x :: _ => convertParam(pType, x)
      case x: Int => IntegerParameter(x)
      case x: Long => IntegerParameter(x.toInt)
      case x: String => try {
        IntegerParameter(x.toInt)
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
      case x: String => DateTimeParameter(x)
      case x => throw new MissingParameter(ParameterError(
        s"unexpected value $x (${x.getClass.getName}) for an SQL DATE parameter", -1
      ))
    }
    case TIMESTAMP(_) => value match {
      case x :: _ => convertParam(pType, x)
      case x: String => DateTimeParameter(x)
      case x => throw new MissingParameter(ParameterError(
        s"unexpected value $x (${x.getClass.getName}) for an SQL TIMESTAMP parameter", -1
      ))
    }
    case SET(t) => value match {
      case x: Seq[_] => SetParameter(x.map(x =>convertParam(t, x)).toSet)
      case x => SetParameter(Set(convertParam(t, x)))
    }
    case RANGE(t) => value match {
      case (a,b) => RangeParameter(convertParam(t, a) ,convertParam(t, b))
      case a :: b :: _ => RangeParameter(convertParam(t, a), convertParam(t, b))
      case x => throw new MissingParameter(ParameterError(
        s"unexpected value $x (${x.getClass.getName}) for an SQL RANGE parameter", -1
      ))
    }
  }

  def from(nullable: Boolean): PartialFunction[String, Type] = {
    case "varchar" | "char" | "bpchar" | "string" => STRING(nullable)
    case x if x.contains("varchar") => STRING(nullable)
    case "int4" | "integer" => INTEGER(nullable)
    case "float" | "float4" | "numeric" | "decimal" => DECIMAL(nullable)
    case "timestamp" | "timestamptz" | "timestamp with time zone" => TIMESTAMP(nullable)
    case "date" => DATE(nullable)
    case "boolean" => BOOLEAN(nullable)
  }
}
trait FilledParameter
case class DateTimeParameter(value : String) extends FilledParameter
case class SetParameter(value : Set[FilledParameter]) extends FilledParameter
case class RangeParameter(low : FilledParameter, High : FilledParameter) extends FilledParameter
case class StringParameter(value : String) extends FilledParameter
case class IntegerParameter(value : Int) extends FilledParameter
