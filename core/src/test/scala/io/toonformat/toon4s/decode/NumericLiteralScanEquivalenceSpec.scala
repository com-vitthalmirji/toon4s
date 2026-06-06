package io.toonformat.toon4s
package decode

import scala.util.Random

import io.toonformat.toon4s.JsonValue.JNumber
import io.toonformat.toon4s.decode.parsers.PrimitiveParser
import munit.FunSuite

/**
 * The hand-rolled numeric scan in PrimitiveParser must classify tokens exactly like the original
 * regex plus leading-zero check. We observe the decision through parsePrimitiveToken, which returns
 * a JNumber only when the token is a valid numeric literal.
 */
class NumericLiteralScanEquivalenceSpec extends FunSuite {

  private val numRegex = "^-?[0-9]+(?:\\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$".r

  private def oldIsNumeric(token: String): Boolean =
    if (token.isEmpty) false
    else {
      val unsigned = if (token.head == '-' || token.head == '+') token.tail else token
      val hasLeadingZero = unsigned.length > 1 && unsigned.head == '0' && unsigned(1).isDigit
      unsigned.nonEmpty && !hasLeadingZero && numRegex.matches(token)
    }

  private def newIsNumeric(token: String): Boolean =
    PrimitiveParser.parsePrimitiveToken(token) match {
    case _: JNumber => true
    case _          => false
    }

  private val curated = List(
    "0",
    "00",
    "007",
    "0.7",
    "0e5",
    "-0",
    "-07",
    "42",
    "-3.14",
    "1.23e10",
    "-5.67E-8",
    "+5",
    "1.",
    ".5",
    "1e",
    "1e-3",
    "12.34.56",
    "--5",
    "abc",
    "1.0",
    "100",
    "1E10",
    "0.0",
    "9",
    "-",
    "1.5e3",
    "e5",
    "1e+",
    "00.5",
    "1234567890",
  )

  test("curated tokens classify identically") {
    curated.foreach(t => assertEquals(newIsNumeric(t), oldIsNumeric(t), s"token [$t]"))
  }

  test("random numeric-shaped tokens classify identically") {
    val alphabet = "0123456789+-.eE".toCharArray
    val rnd = new Random(99L)
    var i = 0
    while (i < 200000) {
      val len = rnd.nextInt(9)
      val sb = new StringBuilder(len)
      var j = 0
      while (j < len) { sb.append(alphabet(rnd.nextInt(alphabet.length))); j += 1 }
      val t = sb.toString
      if (t.nonEmpty) assertEquals(newIsNumeric(t), oldIsNumeric(t), s"token [$t]")
      i += 1
    }
  }

}
