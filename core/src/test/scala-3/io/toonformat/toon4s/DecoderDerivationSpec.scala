package io.toonformat.toon4s

import io.toonformat.toon4s.codec.*
import munit.FunSuite

class DecoderDerivationSpec extends FunSuite {

  test("scala 3 Decoder.derived decodes case class from JsonValue via ToonTyped.decodeAs") {
    case class User(id: Int, name: String) derives Decoder
    case class Data(users: List[User]) derives Decoder

    val toon =
      """users[2]{id,name}:
        |  1,Ada
        |  2,Bob
        |""".stripMargin

    val out = ToonTyped.decodeAs[Data](toon)
    assert(out.isRight)
    val data = out.toOption.get
    assertEquals(data.users.map(_.name), List("Ada", "Bob"))
  }

}
