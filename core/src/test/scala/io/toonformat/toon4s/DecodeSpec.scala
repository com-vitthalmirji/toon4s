package io.toonformat.toon4s

import scala.collection.immutable.VectorMap

import io.toonformat.toon4s.JsonValue._
import io.toonformat.toon4s.error.DecodeError
import munit.FunSuite

class DecodeSpec extends FunSuite {

  test("decode tabular array into objects") {
    val input =
      """users[2]{id,name}:
        |  1,Alice
        |  2,Bob
        |""".stripMargin

    val expected = JObj(
      VectorMap(
        "users" -> JArray(
          Vector(
            JObj(VectorMap("id" -> JNumber(BigDecimal(1)), "name" -> JString("Alice"))),
            JObj(VectorMap("id" -> JNumber(BigDecimal(2)), "name" -> JString("Bob"))),
          )
        )
      )
    )

    assertEquals(Toon.decode(input), Right(expected))
  }

  test("decode list array of objects") {
    val input =
      """items[2]:
        |  - id: 1
        |    name: First
        |  - id: 2
        |    name: Second
        |""".stripMargin

    val expected = JObj(
      VectorMap(
        "items" -> JArray(
          Vector(
            JObj(VectorMap("id" -> JNumber(BigDecimal(1)), "name" -> JString("First"))),
            JObj(VectorMap("id" -> JNumber(BigDecimal(2)), "name" -> JString("Second"))),
          )
        )
      )
    )

    assertEquals(Toon.decode(input), Right(expected))
  }

  test("decode list array of primitives") {
    val input =
      """tags[3]:
        |  - alpha
        |  - beta
        |  - gamma
        |""".stripMargin

    val expected = JObj(
      VectorMap(
        "tags" -> JArray(
          Vector(JString("alpha"), JString("beta"), JString("gamma"))
        )
      )
    )

    assertEquals(Toon.decode(input), Right(expected))
  }

  test("strict length validation for list arrays") {
    val input =
      """tags[1]:
        |  - alpha
        |  - beta
        |""".stripMargin

    val result = Toon.decode(input)
    assert(result.isLeft)
    assert(result.left.exists(_.isInstanceOf[DecodeError.Range]))
  }

  test("blank lines inside tabular array rejected in strict mode") {
    val input =
      """users[2]{id,name}:
        |  1,Alice
        |
        |  2,Bob
        |""".stripMargin

    val result = Toon.decode(input)
    assert(result.isLeft)
    assert(result.left.exists(_.isInstanceOf[DecodeError.Syntax]))
  }

  test("unicode escapes are rejected (TOON v2.1 spec compliance)") {
    // Avoid a literal \uXXXX sequence in source to prevent the Scala 2.13
    // unicode preprocessor from interpreting it at compile time.
    val input = "name: \"test\\u004" + "1value\""

    val result = Toon.decode(input)
    assert(result.isLeft, "Unicode escape \\u0041 should be rejected")
    result.left.foreach {
      err =>
        assert(err.isInstanceOf[DecodeError.Syntax])
        assert(err.message.contains("Invalid escape sequence: \\u"))
    }
  }

  test("all valid TOON escapes are accepted") {
    // Triple-quoted: backslashes are literal. TOON sees: "quote:\" slash:\\\\ newline:\n tab:\t"
    val input = """text: "quote:\" slash:\\\\ newline:\n tab:\t""""

    val result = Toon.decode(input)
    assert(result.isRight)
    result.foreach {
      case JObj(fields) =>
        fields.get("text") match {
        case Some(JString(s)) =>
          // After TOON unescape: \" -> ", \\\\ -> \\, \n -> newline, \t -> tab
          assert(s.contains("quote:\""))
          assert(s.contains("slash:\\\\"))
          assert(s.contains("\n"))
          assert(s.contains("\t"))
        case other => fail(s"Expected JString, got $other")
        }
      case other => fail(s"Expected JObj, got $other")
    }
  }

  test("invalid escape sequences are rejected") {
    val invalidEscapes = List(
      ("\\x", "message: \"test\\xvalue\""),
      ("\\a", "message: \"test\\avalue\""),
      ("\\b", "message: \"test\\bvalue\""),
      ("\\f", "message: \"test\\fvalue\""),
      ("\\v", "message: \"test\\vvalue\""),
      ("\\0", "message: \"test\\0value\""),
    )

    invalidEscapes.foreach {
      case (escape, input) =>
        val result = Toon.decode(input)
        assert(result.isLeft, s"Escape $escape should be rejected")
        result.left.foreach {
          err =>
            assert(err.isInstanceOf[DecodeError.Syntax])
            assert(err.message.contains("Invalid escape sequence"))
        }
    }
  }

}
