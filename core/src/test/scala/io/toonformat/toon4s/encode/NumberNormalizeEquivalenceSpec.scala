package io.toonformat.toon4s
package encode

import scala.util.Random

import io.toonformat.toon4s.JsonValue._
import munit.FunSuite

/**
 * The integer fast path in normalizeNumber must produce output byte-identical to the original
 * stripTrailingZeros plus toPlainString form, for every numeric value.
 */
class NumberNormalizeEquivalenceSpec extends FunSuite {

  private def oldNormalize(n: BigDecimal): String = {
    val s = n.bigDecimal.stripTrailingZeros.toPlainString
    if (s == "-0") "0" else s
  }

  private def check(n: BigDecimal): Unit =
    assertEquals(Primitives.encodePrimitive(JNumber(n), Delimiter.Comma), oldNormalize(n), s"n=$n")

  private val curated = List(
    BigDecimal(0),
    BigDecimal(-0.0),
    BigDecimal(1),
    BigDecimal(-1),
    BigDecimal(100),
    BigDecimal(-100),
    BigDecimal(Long.MaxValue),
    BigDecimal(Long.MinValue),
    BigDecimal("1E5"),
    BigDecimal("1E18"),
    BigDecimal("1E19"),
    BigDecimal("1E20"),
    BigDecimal("100.00"),
    BigDecimal("123.4500"),
    BigDecimal("-0.5000"),
    BigDecimal("0.001"),
    BigDecimal("1.5e3"),
    BigDecimal("1e-3"),
    BigDecimal("999999999999999999"),
    BigDecimal("1000000000000000000"),
    BigDecimal("9999999999999999999"),
    BigDecimal(BigInt("100000000000000000000000")),
    BigDecimal("0.0"),
    BigDecimal("-0.0"),
  )

  test("curated numbers are byte-identical") {
    curated.foreach(check)
  }

  test("random longs are byte-identical") {
    val rnd = new Random(1L)
    (0 until 50000).foreach(_ => check(BigDecimal(rnd.nextLong())))
  }

  test("random decimals and big integers are byte-identical") {
    val rnd = new Random(2L)
    (0 until 50000).foreach { _ =>
      val unscaled = BigInt(rnd.nextInt(40) + 1, rnd) * (if (rnd.nextBoolean()) 1 else -1)
      val scale = rnd.nextInt(45) - 15
      check(BigDecimal(new java.math.BigDecimal(unscaled.bigInteger, scale)))
    }
  }

}
