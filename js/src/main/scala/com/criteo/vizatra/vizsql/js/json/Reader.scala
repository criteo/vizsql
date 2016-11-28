package com.criteo.vizatra.vizsql.js.json

import scala.scalajs.js.{Dynamic, JSON}

trait Reader[T] {
  def apply(dyn: Dynamic): T
  def apply(input: String): T = apply(JSON.parse(input))
}








