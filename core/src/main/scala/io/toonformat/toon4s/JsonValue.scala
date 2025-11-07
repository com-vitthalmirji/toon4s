package io.toonformat.toon4s

sealed trait JsonValue

object JsonValue {
  final case class JString(value: String)                    extends JsonValue
  final case class JNumber(value: BigDecimal)                extends JsonValue
  final case class JBool(value: Boolean)                     extends JsonValue
  case object JNull                                          extends JsonValue
  final case class JArray(value: Vector[JsonValue])          extends JsonValue
  final case class JObj(value: VectorMap[String, JsonValue]) extends JsonValue
}
