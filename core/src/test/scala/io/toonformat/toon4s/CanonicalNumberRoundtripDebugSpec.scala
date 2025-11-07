package io.toonformat.toon4s

import io.toonformat.toon4s.JsonValue._
import munit.FunSuite
import scala.collection.immutable.VectorMap

class CanonicalNumberRoundtripDebugSpec extends FunSuite {

  test("debug: see actual decode structure with 'num' key") {
    val input    = BigDecimal("1e6")
    val original = JObj(VectorMap("num" -> JNumber(input)))
    val encoded  = Toon.encode(original).getOrElse("")

    println(s"\nOriginal: $original")
    println(s"Encoded:\n$encoded")

    val decoded = Toon.decode(encoded)
    println(s"Decoded: $decoded")

    decoded match {
      case Right(result) =>
        println(s"Decoded result type: ${result.getClass.getName}")
        println(s"Decoded result: $result")
        result match {
          case JObj(fields) =>
            println(s"Object has ${fields.size} fields")
            println(s"Field keys: ${fields.keys.mkString(", ")}")
            fields.foreach {
              case (k, v) =>
                println(s"  $k -> $v (${v.getClass.getSimpleName})")
            }
          case other        =>
            println(s"Not a JObj, got: $other")
        }
      case Left(err)     =>
        println(s"Decode error: ${err.message}")
    }
  }
}
