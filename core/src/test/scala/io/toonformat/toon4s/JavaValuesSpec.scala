package io.toonformat.toon4s

import java.util.{ArrayList, Arrays, Collections, LinkedHashMap, LinkedHashSet, Optional}

import scala.collection.immutable.VectorMap

import io.toonformat.toon4s.JsonValue._
import io.toonformat.toon4s.internal.Normalize
import munit.FunSuite

/** Java values given to the encoder are normalized like their Scala counterparts. */
class JavaValuesSpec extends FunSuite {

  private def encode(value: Any): Either[String, String] = Toon.encode(value).left.map(_.message)

  private def ordered(entries: (String, Any)*): LinkedHashMap[String, Any] = {
    val map = new LinkedHashMap[String, Any]()
    entries.foreach { case (k, v) => map.put(k, v) }
    map
  }

  test("a java.util.Map is an object and keeps its iteration order") {
    assertEquals(encode(ordered("z" -> 1, "a" -> 2, "m" -> 3)), Right("z: 1\na: 2\nm: 3"))
  }

  test("a java.util.List is an array") {
    assertEquals(encode(ordered("tags" -> Arrays.asList("jazz", "chill", "lofi"))), Right("tags[3]: jazz,chill,lofi"))
  }

  test("a java.util.Set is an array") {
    val set = new LinkedHashSet[String]()
    set.add("a")
    set.add("b")
    assertEquals(encode(ordered("letters" -> set)), Right("letters[2]: a,b"))
  }

  test("empty Java collections are an empty object and an empty array") {
    assertEquals(
      Normalize.toJson(ordered("map" -> Collections.emptyMap(), "list" -> new ArrayList[Int]())),
      JObj(VectorMap("map" -> JObj(VectorMap.empty), "list" -> JArray(Vector.empty))),
    )
  }

  test("Java maps of Java lists are tabular, the same as their Scala counterparts") {
    val rows = Arrays.asList(ordered("id" -> 1, "name" -> "Ada"), ordered("id" -> 2, "name" -> "Bob"))
    val scala = Map("users" -> Vector(VectorMap("id" -> 1, "name" -> "Ada"), VectorMap("id" -> 2, "name" -> "Bob")))
    assertEquals(encode(ordered("users" -> rows)), encode(scala))
    assertEquals(encode(ordered("users" -> rows)), Right("users[2]{id,name}:\n  1,Ada\n  2,Bob"))
  }

  test("Java values nested inside Scala collections are normalized too") {
    assertEquals(
      Normalize.toJson(Map("outer" -> ordered("list" -> Arrays.asList(1, 2)))),
      JObj(VectorMap("outer" -> JObj(VectorMap("list" -> JArray(Vector(JNumber(1), JNumber(2))))))),
    )
  }

  test("java.math.BigDecimal is a number") {
    assertEquals(Normalize.toJson(new java.math.BigDecimal("12.50")), JNumber(BigDecimal("12.50")))
  }

  test("java.math.BigInteger follows the same safe-integer rule as BigInt") {
    assertEquals(Normalize.toJson(java.math.BigInteger.valueOf(42)), JNumber(42))
    val huge = new java.math.BigInteger("123456789012345678901234567890")
    assertEquals(Normalize.toJson(huge), Normalize.toJson(BigInt(huge)))
  }

  test("java.util.Optional is its value, or null when empty") {
    assertEquals(Normalize.toJson(Optional.of("x")), JString("x"))
    assertEquals(Normalize.toJson(Optional.empty()), JNull)
  }

}
