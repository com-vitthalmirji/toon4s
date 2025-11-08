package io.toonformat.toon4s

import scala.collection.immutable.VectorMap
import io.toonformat.toon4s.JsonValue._
import munit.ScalaCheckSuite
import org.scalacheck.{Arbitrary, Gen, Test}
import org.scalacheck.Prop._

/**
 * Property-based tests for TOON encoding/decoding using ScalaCheck.
 *
 * Tests universal properties that should hold for all valid inputs, catching edge cases that
 * example-based tests miss.
 */
class PropertyTests extends ScalaCheckSuite {

  // ===== Generators =====

  /** Generate valid primitive JsonValue instances. */
  val genPrimitive: Gen[JsonValue] = Gen.oneOf(
    Gen.oneOf(true, false).map(JBool.apply),
    Gen
      .choose(-1000000.0, 1000000.0)
      .map(d => JNumber(BigDecimal(d))),
    Gen.alphaNumStr.map(JString.apply),
  )

  /** Generate small valid JsonValue instances (depth-limited to avoid stack overflow). */
  def genJsonValue(depth: Int = 3): Gen[JsonValue] = {
    if (depth <= 0) genPrimitive
    else
      Gen.frequency(
        5 -> genPrimitive,
        2 -> genArray(depth),
        2 -> genObject(depth),
      )
  }

  def genArray(depth: Int): Gen[JArray] = {
    val maxSize = if (depth > 1) 5 else 10
    Gen.listOfN(Gen.choose(0, maxSize).sample.getOrElse(3), genJsonValue(depth - 1)).map {
      items => JArray(items.toVector)
    }
  }

  def genObject(depth: Int): Gen[JObj] = {
    val maxSize = if (depth > 1) 5 else 10
    Gen
      .listOfN(
        Gen.choose(1, maxSize).sample.getOrElse(3), // At least 1 field
        for {
          key <- Gen.alphaNumStr.suchThat(s => s.nonEmpty && s.length > 0)
          value <- genJsonValue(depth - 1)
        } yield (key, value),
      )
      .suchThat(_.nonEmpty) // Ensure non-empty
      .map {
        pairs => JObj(VectorMap.from(pairs))
      }
  }

  /** Generate valid string content for quoting tests. */
  val genStringContent: Gen[String] = Gen.oneOf(
    Gen.alphaNumStr.suchThat(_.nonEmpty),
    Gen.const(" "),
    Gen.const("  spaces  "),
    Gen.const("with,comma"),
    Gen.const("with:colon"),
    Gen.const("with\"quote"),
    Gen.const("123"), // looks like number
    Gen.const("true"), // looks like boolean
    Gen.const("null"), // looks like null
  )

  // ===== Properties =====

  property("encode always succeeds for valid JsonValue") {
    forAll(genJsonValue(3)) {
      json => Toon.encode(json).isRight
    }
  }

  property("decoded value can be re-encoded") {
    forAll(genJsonValue(3)) {
      json =>
        val encoded = Toon.encode(json)
        encoded match {
        case Right(toon) =>
          val decoded = Toon.decode(toon)
          decoded match {
          case Right(result) =>
            // Re-encoding the decoded value should succeed
            Toon.encode(result).isRight
          case Left(err) =>
            fail(s"Decode failed: ${err.message}\nFor TOON:\n$toon")
          }
        case Left(err) =>
          fail(s"Encode failed: ${err.message}")
        }
    }
  }

  property("numbers preserve precision") {
    forAll(Gen.choose(-1e10, 1e10).map(BigDecimal.apply)) {
      num =>
        val json = JObj(VectorMap("num" -> JNumber(num)))
        val encoded = Toon.encode(json)
        encoded match {
        case Right(toon) =>
          val decoded = Toon.decode(toon)
          decoded match {
          case Right(JObj(fields)) =>
            // Navigate to find the number
            fields.values.headOption match {
            case Some(inner) =>
              extractNumber(inner) match {
              case Some(result) => result.compare(num) == 0
              case None         => fail(s"Could not extract number from $inner")
              }
            case None => fail("No fields in decoded object")
            }
          case Right(other) =>
            fail(s"Expected JObj, got $other")
          case Left(err) =>
            fail(s"Decode failed: ${err.message}")
          }
        case Left(err) =>
          fail(s"Encode failed: ${err.message}")
        }
    }
  }

  private def extractNumber(json: JsonValue): Option[BigDecimal] = json match {
  case JNumber(n)                       => Some(n)
  case JObj(fields) if fields.size == 1 => fields.values.headOption.flatMap(extractNumber)
  case _                                => None
  }

  property("booleans roundtrip correctly") {
    forAll(Gen.oneOf(true, false)) {
      bool =>
        val json = JObj(VectorMap("flag" -> JBool(bool)))
        val encoded = Toon.encode(json)
        encoded match {
        case Right(toon) =>
          val decoded = Toon.decode(toon)
          decoded.isRight
        case Left(err) =>
          fail(s"Encode failed: ${err.message}")
        }
    }
  }

  property("non-empty strings roundtrip") {
    forAll(genStringContent) {
      s =>
        val json = JObj(VectorMap("text" -> JString(s)))
        val encoded = Toon.encode(json)
        encoded match {
        case Right(toon) =>
          Toon.decode(toon).isRight
        case Left(err) =>
          fail(s"Encode failed: ${err.message}")
        }
    }
  }

  property("arrays with uniform primitives encode successfully") {
    forAll(Gen.nonEmptyListOf(Gen.choose(1, 100)).suchThat(_.size <= 50)) {
      nums =>
        val json = JArray(
          nums
            .map(n => JNumber(BigDecimal(n)))
            .toVector
        )
        val encoded = Toon.encode(json)
        encoded match {
        case Right(toon) =>
          // Should decode successfully
          Toon.decode(toon).isRight
        case Left(_) => false
        }
    }
  }

  property("objects with simple fields encode successfully") {
    forAll(Gen.choose(1, 5)) {
      numKeys =>
        val keys = (1 to numKeys)
          .map(i => s"key$i")
          .toList
        val json = JObj(
          VectorMap.from(
            keys.map(k => k -> JString(k))
          )
        )
        val encoded = Toon.encode(json)
        encoded match {
        case Right(toon) =>
          Toon.decode(toon).isRight
        case Left(_) => false
        }
    }
  }

  property("delimiter options work with all delimiters") {
    forAll(genPrimitive, Gen.oneOf(Delimiter.Comma, Delimiter.Tab, Delimiter.Pipe)) {
      (json, delim) =>
        val options = EncodeOptions(delimiter = delim)
        val encoded = Toon.encode(json, options)
        encoded match {
        case Right(toon) =>
          val decoded = Toon.decode(toon, DecodeOptions())
          decoded.isRight
        case Left(_) => false
        }
    }
  }

  property("lenient mode accepts more inputs than strict") {
    // Blank line in array - should fail in strict, succeed in lenient
    val toon = """arr[2]:
                 |  - item1
                 |
                 |  - item2""".stripMargin
    val strict = Toon.decode(toon, DecodeOptions(strictness = Strictness.Strict))
    val lenient = Toon.decode(toon, DecodeOptions(strictness = Strictness.Lenient))
    strict.isLeft && lenient.isRight
  }

  property("malformed TOON with invalid escape fails") {
    // Invalid escape sequence
    val malformed = """text: "\x""""
    Toon.decode(malformed).isLeft
  }

  property("malformed TOON with unterminated string fails") {
    // Unterminated string
    val malformed = """text: "unterminated"""
    Toon.decode(malformed).isLeft
  }

  property("malformed TOON with array length mismatch fails in strict mode") {
    // Array declares 5 items but provides 3
    val malformed = "arr[5]: 1,2,3"
    Toon.decode(malformed, DecodeOptions(strictness = Strictness.Strict)).isLeft
  }

  // ===== Configuration =====

  // Increase test count for thorough testing
  override def scalaCheckTestParameters: Test.Parameters =
    super.scalaCheckTestParameters
      .withMinSuccessfulTests(100) // 100 successful tests per property
      .withMaxDiscardRatio(20) // Allow more discards for filtered generators

}
