package io.toonformat.toon4s

import scala.collection.immutable.VectorMap

import io.toonformat.toon4s.JsonValue._
import munit.FunSuite

class JavaQuickstartSpec extends FunSuite {

  private val usersToon = "users[2]{id,name}:\n  1,Ada\n  2,Bob"

  test("the Java example encodes Java maps and lists as tabular TOON") {
    assertEquals(JavaQuickstart.encode(), usersToon)
  }

  test("the Java example decodes TOON back to the same tree") {
    assertEquals(
      JavaQuickstart.decode(usersToon),
      JObj(
        VectorMap(
          "users" -> JArray(
            Vector(
              JObj(VectorMap("id" -> JNumber(1), "name" -> JString("Ada"))),
              JObj(VectorMap("id" -> JNumber(2), "name" -> JString("Bob"))),
            )
          )
        )
      ),
    )
  }

  test("the Java example reports a decode error through ToonResult") {
    val message = JavaQuickstart.decodeError("key: value\n\tnested: wrong")
    assert(message != null && message.contains("Tabs are not allowed"), String.valueOf(message))
  }

}
