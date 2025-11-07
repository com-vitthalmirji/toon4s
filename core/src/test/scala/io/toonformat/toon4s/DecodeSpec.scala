package io.toonformat.toon4s

import io.toonformat.toon4s.JsonValue._
import io.toonformat.toon4s.error.DecodeError
import munit.FunSuite
import scala.collection.immutable.VectorMap

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
            JObj(VectorMap("id" -> JNumber(BigDecimal(2)), "name" -> JString("Bob")))
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
            JObj(VectorMap("id" -> JNumber(BigDecimal(2)), "name" -> JString("Second")))
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
}
