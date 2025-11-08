package io.toonformat.toon4s

import io.toonformat.toon4s.JsonValue._
import io.toonformat.toon4s.codec.*
import munit.FunSuite

class CodecDerivationSpec extends FunSuite {

  test("scala 3 Encoder.derived encodes case class to JsonValue") {
    case class User(id: Int, name: String)
    case class Data(users: List[User]) derives Encoder

    val data = Data(List(User(1, "Ada"), User(2, "Bob")))
    val json = summon[Encoder[Data]].apply(data)
    val expected = JObj(
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
    assertEquals(json, expected)
  }

}
