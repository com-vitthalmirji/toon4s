package io.toonformat.toon4s

import scala.collection.immutable.VectorMap

import io.toonformat.toon4s.JsonValue._
import munit.FunSuite

class CanonicalNumberDebugSpec extends FunSuite {

  test("debug: see actual encoding output") {
    val testCases = List(
      ("exponent", BigDecimal("1e6")),
      ("trailing zeros", BigDecimal("1.5000")),
      ("negative zero", BigDecimal("-0")),
      ("plain", BigDecimal("42")),
    )

    testCases.foreach {
      case (label, input) =>
        val json = JObj(VectorMap("value" -> JNumber(input)))
        val encoded = Toon.encode(json).getOrElse("")
        println(s"\n=== $label: $input ===")
        println(encoded)
        println("===")
    }
  }

}
