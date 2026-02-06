package io.toonformat.toon4s

import scala.collection.immutable.VectorMap

import io.toonformat.toon4s.JsonValue._
import munit.FunSuite

class SpecSyncRegressionSpec extends FunSuite {

  test("decode accepts zero exponent forms as numbers") {
    val input =
      """value: 0e1
        |other: -0e1
        |""".stripMargin

    val expected = JObj(
      VectorMap(
        "value" -> JNumber(BigDecimal(0)),
        "other" -> JNumber(BigDecimal(0)),
      )
    )

    assertEquals(Toon.decode(input), Right(expected))
  }

  test("decode parses nested list array under first list-item field") {
    val input =
      """items[1]:
        |  - matrix[2]:
        |      - [2]: 1,2
        |      - [2]: 3,4
        |    name: grid
        |""".stripMargin

    val expected = JObj(
      VectorMap(
        "items" -> JArray(
          Vector(
            JObj(
              VectorMap(
                "matrix" -> JArray(
                  Vector(
                    JArray(Vector(JNumber(BigDecimal(1)), JNumber(BigDecimal(2)))),
                    JArray(Vector(JNumber(BigDecimal(3)), JNumber(BigDecimal(4)))),
                  )
                ),
                "name" -> JString("grid"),
              )
            )
          )
        )
      )
    )

    assertEquals(Toon.decode(input), Right(expected))
  }

  test("encode keeps +2 depth for first list-item array field rows") {
    val input = Map(
      "items" -> Vector(
        Map(
          "matrix" -> Vector(
            Vector(1, 2),
            Vector(3, 4),
          ),
          "name" -> "grid",
        )
      )
    )

    val expected =
      """items[1]:
        |  - matrix[2]:
        |      - [2]: 1,2
        |      - [2]: 3,4
        |    name: grid""".stripMargin

    assertEquals(Toon.encode(input), Right(expected))
  }

  test("encode writes empty object list item as bare hyphen") {
    val input = Map(
      "items" -> Vector(
        "first",
        "second",
        Map.empty[String, Any],
      )
    )

    val expected =
      """items[3]:
        |  - first
        |  - second
        |  -""".stripMargin

    assertEquals(Toon.encode(input), Right(expected))
  }
}
