package io.toonformat.toon4s

// Type-safe JSON ADT
import scala.collection.immutable.VectorMap

object jsonsyntax {
  import JsonValue._
  def obj(fields: (String, JsonValue)*): JsonValue = JObj(VectorMap.from(fields))
  def arr(items: JsonValue*): JsonValue            = JArray(items.toVector)
  def str(s: String): JsonValue                    = JString(s)
  def num(n: BigDecimal): JsonValue                = JNumber(n)
  def bool(b: Boolean): JsonValue                  = JBool(b)
  val nul: JsonValue                               = JNull
}
