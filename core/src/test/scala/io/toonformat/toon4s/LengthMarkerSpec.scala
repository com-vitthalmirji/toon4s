package io.toonformat.toon4s

import scala.collection.immutable.VectorMap

import io.toonformat.toon4s.JsonValue._
import munit.FunSuite

class LengthMarkerSpec extends FunSuite {

  test("array header includes # when length marker enabled") {
    val data = JObj(VectorMap("items" -> JArray(Vector(JNumber(1), JNumber(2)))))
    val options = EncodeOptions.withLengthMarker(EncodeOptions(), enabled = true)
    val encoded = Toon.encode(data, options).getOrElse("")
    assert(encoded.contains("[#2]"))
  }

  test("array header omits # by default") {
    val data = JObj(VectorMap("items" -> JArray(Vector(JNumber(1), JNumber(2)))))
    val encoded = Toon.encode(data).getOrElse("")
    assert(encoded.contains("[2]"))
    assert(!encoded.contains("[#"))
  }

  test("tabular header includes # when length marker enabled") {
    val rows = Vector(
      JObj(VectorMap("id" -> JNumber(1), "name" -> JString("Alice"))),
      JObj(VectorMap("id" -> JNumber(2), "name" -> JString("Bob"))),
    )
    val data = JObj(VectorMap("users" -> JArray(rows)))
    val options = EncodeOptions.withLengthMarker(EncodeOptions(), enabled = true)
    val encoded = Toon.encode(data, options).getOrElse("")
    assert(encoded.contains("users[#2]{id,name}:"))
  }

  test("builder enables length marker") {
    val options = EncodeOptionsBuilder.empty
      .indent(2)
      .delimiter(Delimiter.Comma)
      .withLengthMarker(enabled = true)
      .build

    val data = JObj(VectorMap("items" -> JArray(Vector(JNumber(1), JNumber(2)))))
    val encoded = Toon.encode(data, options).getOrElse("")
    assert(encoded.contains("[#2]"))
    assert(EncodeOptions.usesLengthMarker(options))
  }

  test("decoder accepts # in array headers") {
    val toon = "items[#2]: 1,2"
    val result = Toon.decode(toon)
    assert(result.isRight)
  }

  test("round trip works with length marker enabled") {
    val data = JObj(VectorMap("items" -> JArray(Vector(JNumber(1), JNumber(2), JNumber(3)))))
    val options = EncodeOptions.withLengthMarker(EncodeOptions(), enabled = true)
    val encoded = Toon.encode(data, options).getOrElse("")
    val decoded = Toon.decode(encoded)
    assertEquals(decoded, Right(data))
  }

}
