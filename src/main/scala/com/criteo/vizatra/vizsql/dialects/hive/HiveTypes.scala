package com.criteo.vizatra.vizsql.hive

import com.criteo.vizatra.vizsql.{Column, Type}

case class HiveArray(elem: Type) extends Type {
  val nullable = true

  def withNullable(nullable: Boolean) = this

  def canBeCastTo(other: Type) = this == other

  def show = s"array<${elem.show}>"
}

case class HiveStruct(elems: List[Column]) extends Type {
  val nullable = true

  def withNullable(nullable: Boolean) = this

  def canBeCastTo(other: Type) = this == other

  def show = s"struct<${elems.map { col => s"${col.name}:${col.typ.show}" }.mkString(",")}>"
}

case class HiveMap(key: Type, value: Type) extends Type {
  val nullable = true

  def withNullable(nullable: Boolean) = this

  def canBeCastTo(other: Type) = this == other

  def show = s"map<${key.show},${value.show}>"
}

case class HiveUDTFResult(types: List[Type]) extends Type {
  val nullable = true

  def withNullable(nullable: Boolean) = this

  def canBeCastTo(other: Type) = this == other

  def show = s"udtfresult<${types.map(_.show).mkString(",")}>"
}
