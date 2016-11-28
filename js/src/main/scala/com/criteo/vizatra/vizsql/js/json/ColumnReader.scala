package com.criteo.vizatra.vizsql.js.json

import com.criteo.vizatra.vizsql._
import com.criteo.vizatra.vizsql.hive.TypeParser

import scala.scalajs.js.{Dynamic, UndefOr}

object ColumnReader extends Reader[Column] {
  lazy val hiveTypeParser = new TypeParser

  override def apply(dyn: Dynamic): Column = {
    val name = dyn.name.asInstanceOf[String]
    val nullable = dyn.nullable.asInstanceOf[UndefOr[Boolean]].getOrElse(false)
    val typ = parseType(dyn.`type`.asInstanceOf[String], nullable)
    Column(name, typ)
  }

  def parseType(input: String, nullable: Boolean): Type = Type.from(nullable).applyOrElse(input, (rest: String) =>
    hiveTypeParser.parseType(rest) match {
      case Left(err) => throw new IllegalArgumentException(err)
      case Right(t) => t
    }
  )
}
