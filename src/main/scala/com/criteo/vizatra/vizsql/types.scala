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