package io.toonformat.toon4s

import java.io.StringWriter

import io.toonformat.toon4s.JsonValue._
import io.toonformat.toon4s.encode.Encoders
import munit.FunSuite

class StreamEncodeSpec extends FunSuite {

  test("encodeTo(writer) matches encode(string)") {
    val value = JObj(
      scala.collection.immutable.VectorMap(
        "users" -> JArray(
          Vector(
            JObj(
              scala.collection.immutable.VectorMap("id" -> JNumber(1), "name" -> JString("Ada"))
            ),
            JObj(scala.collection.immutable.VectorMap("id" -> JNumber(2), "name" -> JString("Bob"))),
          )
        )
      )
    )
    val opts = EncodeOptions()
    val s1 = Encoders.encode(value, opts)
    val sw = new StringWriter()
    Encoders.encodeTo(value, sw, opts)
    val s2 = sw.toString
    assertEquals(s2, s1)
  }

}
