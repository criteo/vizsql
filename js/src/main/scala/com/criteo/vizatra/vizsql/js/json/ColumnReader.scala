package com.criteo.vizatra.vizsql.js.json

import com.criteo.vizatra.vizsql._
import com.criteo.vizatra.vizsql.hive.{HiveArray, HiveStruct}

import scala.scalajs.js.UndefOr
import scala.scalajs.js.Dynamic

object ColumnReader extends Reader[Column] {
  override def apply(dyn: Dynamic): Column = {
    val name = dyn.name.asInstanceOf[String]
    val nullable = dyn.nullable.asInstanceOf[UndefOr[Boolean]].getOrElse(false)
    val typ = parseType(dyn.`type`.asInstanceOf[String], nullable)
    Column(name, typ)
  }

  def parseType(input: String, nullable: Boolean): Type = Type.from(nullable).applyOrElse(input, (rest: String) => rest match {
    case arrayPattern(t) => HiveArray(parseType(t, nullable))
    case structPattern(cols) => HiveStruct(parseStructCols(cols))
    case _ => throw new Exception(s"invalid type $input")
  })

  def parseStructCols(input: String): List[Column] =
    input.split(",").map {
      case structColPattern(name, colType) => Column(name, parseType(colType, true))
    }.toList

  private val arrayPattern = """array<(.*)>""".r
  private val structPattern = """struct<(.*)>""".r
  private val structColPattern = """^([^:]+):([^:]+)$""".r
}
