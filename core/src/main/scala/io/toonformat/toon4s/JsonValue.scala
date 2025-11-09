package io.toonformat.toon4s

import scala.collection.immutable.VectorMap

/**
 * Abstract syntax tree for JSON-compatible values.
 *
 * Represents the data model returned by TOON decoding. All decoded TOON documents produce a
 * [[JsonValue]] tree that can be pattern matched or traversed.
 *
 * ==Usage==
 * {{{
 * import io.toonformat.toon4s.JsonValue._
 *
 * val json = JObj(VectorMap(
 *   "name" -> JString("Alice"),
 *   "age" -> JNumber(30),
 *   "active" -> JBool(true),
 *   "tags" -> JArray(Vector(JString("admin"), JString("user")))
 * ))
 *
 * // Pattern matching
 * json match {
 *   case JObj(fields) => fields.get("name")
 *   case _ => None
 * }
 * }}}
 *
 * @see
 *   [[io.toonformat.toon4s.JsonValue.JString]] for string values
 * @see
 *   [[io.toonformat.toon4s.JsonValue.JNumber]] for numeric values
 * @see
 *   [[io.toonformat.toon4s.JsonValue.JBool]] for boolean values
 * @see
 *   [[io.toonformat.toon4s.JsonValue.JNull]] for null
 * @see
 *   [[io.toonformat.toon4s.JsonValue.JArray]] for arrays
 * @see
 *   [[io.toonformat.toon4s.JsonValue.JObj]] for objects
 */
sealed trait JsonValue

object JsonValue {

  /**
   * A JSON string value.
   *
   * @param value
   *   The string content
   *
   * @example
   *   {{{
   * JString("hello") // Represents the TOON: value: hello
   * JString("with spaces") // Quoted in TOON: value: "with spaces"
   *   }}}
   */
  final case class JString(value: String) extends JsonValue

  /**
   * A JSON number value using arbitrary-precision decimal.
   *
   * Uses [[scala.math.BigDecimal]] to preserve precision for all numeric values.
   *
   * @param value
   *   The numeric value
   *
   * @example
   *   {{{
   * JNumber(42) // Represents: value: 42
   * JNumber(3.14159) // Represents: value: 3.14159
   * JNumber(BigDecimal("1.23e100")) // Large numbers preserved
   *   }}}
   */
  final case class JNumber(value: BigDecimal) extends JsonValue

  /**
   * A JSON boolean value.
   *
   * @param value
   *   The boolean value (true or false)
   *
   * @example
   *   {{{
   * JBool(true) // Represents: value: true
   * JBool(false) // Represents: value: false
   *   }}}
   */
  final case class JBool(value: Boolean) extends JsonValue

  /**
   * The JSON null value.
   *
   * @example
   *   {{{
   * JNull // Represents: value: null
   *   }}}
   */
  case object JNull extends JsonValue

  /**
   * A JSON array of values.
   *
   * Arrays preserve insertion order and may contain heterogeneous types.
   *
   * @param value
   *   The vector of array elements
   *
   * @example
   *   {{{
   * // Inline array
   * JArray(Vector(JNumber(1), JNumber(2), JNumber(3)))
   * // Represents: arr[3]: 1,2,3
   *
   * // List array
   * JArray(Vector(JString("apple"), JString("banana")))
   * // Represents:
   * // arr[2]:
   * //   - apple
   * //   - banana
   *   }}}
   */
  final case class JArray(value: Vector[JsonValue]) extends JsonValue

  /**
   * A JSON object (key-value mapping).
   *
   * Uses [[scala.collection.immutable.VectorMap]] to preserve field insertion order, which is
   * important for TOON's tabular array representation.
   *
   * @param value
   *   The mapping of field names to values
   *
   * @example
   *   {{{
   * import scala.collection.immutable.VectorMap
   *
   * JObj(VectorMap(
   *   "id" -> JNumber(1),
   *   "name" -> JString("Alice")
   * ))
   * // Represents:
   * // id: 1
   * // name: Alice
   *   }}}
   */
  final case class JObj(value: VectorMap[String, JsonValue]) extends JsonValue

}
