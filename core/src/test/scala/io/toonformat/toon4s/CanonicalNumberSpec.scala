package io.toonformat.toon4s

import io.toonformat.toon4s.JsonValue._
import munit.FunSuite
import scala.collection.immutable.VectorMap

/** Tests for TOON v1.4 §2 canonical number encoding requirements.
  *
  * Spec requirements:
  *   - No exponent notation (1e6 → 1000000)
  *   - No trailing zeros in fractional part (1.5000 → 1.5)
  *   - No leading zeros except "0" (05 is invalid)
  *   - -0 → 0
  */
class CanonicalNumberSpec extends FunSuite {

  test("exponent notation is expanded to plain form") {
    val testCases = List(
      BigDecimal("1e6")       -> "1000000",
      BigDecimal("1E6")       -> "1000000",
      BigDecimal("1.5e3")     -> "1500",
      BigDecimal("1.5E3")     -> "1500",
      BigDecimal("1e-3")      -> "0.001",
      BigDecimal("1E-3")      -> "0.001",
      BigDecimal("2.5e-2")    -> "0.025",
      BigDecimal("123.456e2") -> "12345.6"
    )

    testCases.foreach {
      case (input, expected) =>
        val json    = JObj(VectorMap("num" -> JNumber(input)))
        val encoded = Toon.encode(json).getOrElse("")

        assert(
          encoded.contains(expected),
          s"Expected to find '$expected' in encoded output:\n$encoded"
        )

        // Verify no 'e' or 'E' in number lines
        val lines       = encoded.split('\n').map(_.trim).filter(_.nonEmpty)
        val numberLines = lines.filter(
          line => line.head.isDigit || line.startsWith("-")
        )
        numberLines.foreach {
          line =>
            assert(
              !line.contains('e') && !line.contains('E'),
              s"Number should not contain exponent notation: $line"
            )
        }
    }
  }

  test("trailing zeros in fractional part are removed") {
    val testCases = List(
      BigDecimal("1.5000")      -> "1.5",
      BigDecimal("1.50")        -> "1.5",
      BigDecimal("1.500000000") -> "1.5",
      BigDecimal("123.4500")    -> "123.45",
      BigDecimal("0.1000")      -> "0.1",
      BigDecimal("10.00")       -> "10"
    )

    testCases.foreach {
      case (input, expected) =>
        val json    = JObj(VectorMap("num" -> JNumber(input)))
        val encoded = Toon.encode(json).getOrElse("")

        assert(
          encoded.contains(expected),
          s"Input $input: Expected to find '$expected' in:\n$encoded"
        )
    }
  }

  test("integer values do not have decimal point") {
    val testCases = List(
      BigDecimal("1.0")       -> "1",
      BigDecimal("10.0")      -> "10",
      BigDecimal("100.00")    -> "100",
      BigDecimal("1000.0000") -> "1000",
      BigDecimal(42)          -> "42",
      BigDecimal(0)           -> "0"
    )

    testCases.foreach {
      case (input, expected) =>
        val json    = JObj(VectorMap("num" -> JNumber(input)))
        val encoded = Toon.encode(json).getOrElse("")

        assert(
          encoded.contains(expected),
          s"Input $input: Expected to find '$expected' in:\n$encoded"
        )

        // For integers, verify no decimal point in the number lines
        if (!expected.contains('.')) {
          val lines       = encoded.split('\n').map(_.trim).filter(_.nonEmpty)
          val numberLines = lines.filter(
            line => line.head.isDigit || line.startsWith("-")
          )
          numberLines.foreach {
            line =>
              assert(
                !line.contains('.'),
                s"Integer should not have decimal point: $line"
              )
          }
        }
    }
  }

  test("negative zero is normalized to zero") {
    val testCases = List(
      BigDecimal("-0")     -> "0",
      BigDecimal("-0.0")   -> "0",
      BigDecimal("-0.00")  -> "0",
      BigDecimal("-0.000") -> "0"
    )

    testCases.foreach {
      case (input, expected) =>
        val json    = JObj(VectorMap("num" -> JNumber(input)))
        val encoded = Toon.encode(json).getOrElse("")

        // Should contain "0" and should NOT contain "-0"
        assert(
          encoded.contains(expected) && !encoded.contains("-0"),
          s"Input $input: Expected normalized '0' (no minus) in:\n$encoded"
        )
    }
  }

  test("very large numbers are expanded without exponent") {
    val testCases = List(
      BigDecimal("1e10")         -> "10000000000",
      BigDecimal("9.99e9")       -> "9990000000",
      BigDecimal("1.23456789e8") -> "123456789"
    )

    testCases.foreach {
      case (input, expected) =>
        val json    = JObj(VectorMap("num" -> JNumber(input)))
        val encoded = Toon.encode(json).getOrElse("")

        assert(
          encoded.contains(expected),
          s"Input $input: Expected to find '$expected' in:\n$encoded"
        )
    }
  }

  test("very small numbers are expanded without exponent") {
    val testCases = List(
      BigDecimal("1e-10")   -> "0.0000000001",
      BigDecimal("1.5e-5")  -> "0.000015",
      BigDecimal("9.99e-9") -> "0.00000000999"
    )

    testCases.foreach {
      case (input, expected) =>
        val json    = JObj(VectorMap("num" -> JNumber(input)))
        val encoded = Toon.encode(json).getOrElse("")

        assert(
          encoded.contains(expected),
          s"Input $input: Expected to find '$expected' in:\n$encoded"
        )
    }
  }

  test("canonical encoding round-trips correctly") {
    val testCases = List(
      BigDecimal("1e6"),
      BigDecimal("1.5000"),
      BigDecimal("-0"),
      BigDecimal("123.4500"),
      BigDecimal("1.0"),
      BigDecimal("0.0001"),
      BigDecimal("1e-10")
    )

    testCases.foreach {
      input =>
        val original = JObj(VectorMap("num" -> JNumber(input)))
        val encoded  = Toon.encode(original).getOrElse("")
        val decoded  = Toon.decode(encoded)

        decoded match {
          case Right(result) =>
            // Navigate nested structure to get the number
            result match {
              case JObj(outer) =>
                outer.get("num") match {
                  case Some(innerJson) =>
                    val extracted = extractNumber(innerJson)
                    extracted match {
                      case Some(n) =>
                        // After round-trip, numeric value should be equal
                        assertEquals(
                          n.compare(input),
                          0,
                          s"Round-trip failed for $input: got $n"
                        )
                      case None    => fail(s"Could not extract number from decoded JSON for $input")
                    }
                  case None            => fail(s"No 'num' field in decoded object")
                }
              case other       => fail(s"Expected JObj at root, got $other")
            }
          case Left(err)     => fail(s"Decode failed for $input: ${err.message}")
        }
    }
  }

  // Helper to extract number from potentially nested structure
  private def extractNumber(json: JsonValue): Option[BigDecimal] = json match {
    case JNumber(n)                       => Some(n)
    case JObj(fields) if fields.size == 1 =>
      fields.values.headOption.flatMap(extractNumber)
    case _                                => None
  }

  test("arrays of numbers use canonical form") {
    val numbers = Vector(
      JNumber(BigDecimal("1e3")),
      JNumber(BigDecimal("1.50")),
      JNumber(BigDecimal("-0"))
    )
    val json    = JObj(VectorMap("values" -> JArray(numbers)))
    val encoded = Toon.encode(json).getOrElse("")

    // Should contain canonical forms
    assert(encoded.contains("1000"), s"Should contain expanded 1e3 = 1000 in:\n$encoded")
    assert(encoded.contains("1.5"), s"Should contain 1.5 without trailing zero in:\n$encoded")

    // Verify -0 is normalized
    val lines       = encoded.split('\n').map(_.trim).filter(_.nonEmpty)
    val numberLines = lines.filter(
      line =>
        line.headOption.exists(
          c => c.isDigit || c == '-'
        )
    )
    assert(
      numberLines.exists(_.contains("0")) && !numberLines.exists(_.contains("-0")),
      s"Should normalize -0 to 0 in:\n$encoded"
    )
  }

  test("precision is preserved for decimal numbers") {
    val testCases = List(
      BigDecimal("0.1")        -> "0.1",
      BigDecimal("0.01")       -> "0.01",
      BigDecimal("0.001")      -> "0.001",
      BigDecimal("123.456789") -> "123.456789",
      BigDecimal("0.0000001")  -> "0.0000001"
    )

    testCases.foreach {
      case (input, expected) =>
        val json    = JObj(VectorMap("num" -> JNumber(input)))
        val encoded = Toon.encode(json).getOrElse("")

        assert(
          encoded.contains(expected),
          s"Input $input: Expected to find '$expected' in:\n$encoded"
        )
    }
  }

  test("no leading zeros are produced") {
    // Since we're encoding from BigDecimal, leading zeros shouldn't be possible
    val testCases = List(
      BigDecimal("5")   -> "5",
      BigDecimal("05")  -> "5",
      BigDecimal("007") -> "7",
      BigDecimal("42")  -> "42",
      BigDecimal("0")   -> "0",
      BigDecimal("0.5") -> "0.5"
    )

    testCases.foreach {
      case (input, expected) =>
        val json    = JObj(VectorMap("num" -> JNumber(input)))
        val encoded = Toon.encode(json).getOrElse("")

        assert(
          encoded.contains(expected),
          s"Input $input: Expected to find '$expected' in:\n$encoded"
        )

        // Verify no leading zeros (except standalone "0" or "0.")
        val lines       = encoded.split('\n').map(_.trim).filter(_.nonEmpty)
        val numberLines = lines.filter(
          line =>
            line.headOption.exists(
              c => c.isDigit || c == '-'
            )
        )
        numberLines.foreach {
          line =>
            val trimmed = line.trim
            if (trimmed.length > 1 && trimmed.head == '0' && trimmed(1) != '.') {
              fail(s"Number should not have leading zeros: $line")
            }
        }
    }
  }
}
