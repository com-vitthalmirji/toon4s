package io.toonformat.toon4s
package encode

import io.toonformat.toon4s.JsonValue._
import munit.FunSuite

/**
 * The raw-value formatters must produce output identical to encodePrimitive for the same value.
 * This locks the parity that the Spark direct emitter relies on.
 */
class RawPrimitiveFormatSpec extends FunSuite {

  private val delimiters = List(Delimiter.Comma, Delimiter.Tab, Delimiter.Pipe)

  private def assertParity(formatted: String, json: JsonValue): Unit =
    delimiters.foreach(d => assertEquals(formatted, Primitives.encodePrimitive(json, d)))

  test("formatLong matches encodePrimitive") {
    val values =
      List(0L, 1L, -5L, 100L, 1000000000000L, Long.MaxValue, Long.MinValue)
    values.foreach(n => assertParity(Primitives.formatLong(n), JNumber(BigDecimal(n))))
  }

  test("formatBigInt matches encodePrimitive for values beyond Long") {
    val values = List(
      BigInt("9223372036854775808"),
      BigInt("-9223372036854775809"),
      BigInt("100000000000000000000000"),
    )
    values.foreach(n => assertParity(Primitives.formatBigInt(n), JNumber(BigDecimal(n))))
  }

  test("formatDouble matches encodePrimitive") {
    val values = List(0.0D, -0.0D, 3.14D, 25.5D, 1e10D, 1e-3D, 123.456D)
    values.foreach(d => assertParity(Primitives.formatDouble(d), JNumber(BigDecimal(d))))
  }

  test("formatBigDecimal matches encodePrimitive") {
    val values = List(BigDecimal("1.5000"), BigDecimal("1e6"), BigDecimal("-0"))
    values.foreach(n => assertParity(Primitives.formatBigDecimal(n), JNumber(n)))
  }

  test("formatBoolean and nullToken match encodePrimitive") {
    assertParity(Primitives.formatBoolean(true), JBool(true))
    assertParity(Primitives.formatBoolean(false), JBool(false))
    assertParity(Primitives.nullToken, JNull)
  }

}
