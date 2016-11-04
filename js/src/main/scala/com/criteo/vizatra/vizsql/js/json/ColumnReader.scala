package com.criteo.vizatra.vizsql.js.json

import com.criteo.vizatra.vizsql._

import scala.scalajs.js.UndefOr
import scala.scalajs.js.Dynamic

object ColumnReader extends Reader[Column] {
  override def apply(dyn: Dynamic): Column = {
    val name = dyn.name.asInstanceOf[String]
    val nullable = dyn.nullable.asInstanceOf[UndefOr[Boolean]].getOrElse(false)
    val typ = parseType(dyn.`type`.asInstanceOf[String], nullable)
    Column(name, typ)
  }

  def parseType(input: String, nullable: Boolean): Type = input match {
    case "varchar" | "char" | "bpchar" => STRING(nullable)
    case "int4" | "integer" => INTEGER(nullable)
    case "float" | "float4" | "numeric" => DECIMAL(nullable)
    case "timestamp" | "timestamptz" | "timestamp with time zone" => TIMESTAMP(nullable)
    case "date" => DATE(nullable)
    case "boolean" => BOOLEAN(nullable)
    case _ => throw new Exception(s"invalid type $input")
  }
}
