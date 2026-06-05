package io.toonformat.toon4s
package encode

import scala.util.Random

import munit.FunSuite

/**
 * The hand-rolled character scanners must be byte-identical to the regexes they replaced. The
 * reference here uses the original patterns; production uses the manual scans.
 */
class QuotingScanEquivalenceSpec extends FunSuite {

  private val keyRegex  = "^[A-Za-z_][A-Za-z0-9_.]*$".r
  private val numRegex  = "^-?\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?$".r
  private val zeroRegex = "^0\\d+$".r

  private val structural = Set('"', '\\', '[', ']', '{', '}', '\n', '\r', '\t')

  private def refNumericLike(v: String): Boolean =
    numRegex.matches(v) || zeroRegex.matches(v)

  private def refBooleanOrNull(v: String): Boolean =
    v == "true" || v == "false" || v == "null"

  private def refSafeUnquoted(v: String, delim: Delimiter): Boolean = {
    val basic =
      v.nonEmpty &&
        !Character.isWhitespace(v.charAt(0)) &&
        !Character.isWhitespace(v.charAt(v.length - 1)) &&
        !v.contains(':') &&
        !v.contains(delim.char) &&
        !v.startsWith("-")
    basic &&
    !refBooleanOrNull(v) &&
    !refNumericLike(v) &&
    !v.exists(structural.contains) &&
    !v.exists(_ < ' ')
  }

  private val curated = List(
    "", "true", "false", "null", "0", "00", "01", "007", "123", "-5", "-0", "1.5", "1.",
    "1.5e3", "1e", "1e-3", ".5", "12.", "abc", "a_b.c", "_x", "1abc", "a-b", "a b", " a",
    "a ", "a:b", "a,b", "a|b", "[", "{", "12.34.56", "--5", "+5", "0.0", "0e0", "1E10",
    "name", "id", "x.y.z", "9", "-", "e5", "E", "1e+", "00.5",
  )

  private val delimiters = List(Delimiter.Comma, Delimiter.Tab, Delimiter.Pipe)

  private val alphabet =
    ("abcXYZ012._-+eE. \t:,|[]{}\"\\" + "\n").toCharArray

  private def randomStrings(seed: Long, count: Int): Seq[String] = {
    val rnd = new Random(seed)
    (0 until count).map { _ =>
      val len = rnd.nextInt(8)
      (0 until len).map(_ => alphabet(rnd.nextInt(alphabet.length))).mkString
    }
  }

  test("isValidUnquotedKey matches the original regex") {
    (curated ++ randomStrings(1L, 20000)).foreach { s =>
      assertEquals(Primitives.isValidUnquotedKey(s), keyRegex.matches(s), s"key [$s]")
    }
  }

  test("isSafeUnquoted matches the original regex-based reference") {
    val samples = curated ++ randomStrings(2L, 20000)
    samples.foreach { s =>
      delimiters.foreach { d =>
        assertEquals(Primitives.isSafeUnquoted(s, d), refSafeUnquoted(s, d), s"safe [$s] $d")
      }
    }
  }
}
