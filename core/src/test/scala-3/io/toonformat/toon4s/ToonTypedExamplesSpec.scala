package io.toonformat.toon4s

import java.io.StringWriter

import io.toonformat.toon4s.codec.{Decoder, Encoder}
import io.toonformat.toon4s.error.DecodeError
import munit.FunSuite

/** Runs the `@example` blocks of ToonTyped's Scaladoc, so the documented results stay true. */
class ToonTypedExamplesSpec extends FunSuite {

  case class User(id: Int, name: String) derives Encoder, Decoder

  test("encode example") {
    assertEquals(ToonTyped.encode(User(1, "Ada")), Right("id: 1\nname: Ada"))
  }

  test("encodeTo example") {
    val out = new StringWriter()
    assertEquals(ToonTyped.encodeTo(User(1, "Ada"), out), Right(()))
    assertEquals(out.toString, "id: 1\nname: Ada")
  }

  test("decodeAs example") {
    assertEquals(ToonTyped.decodeAs[User]("id: 1\nname: Ada"), Right(User(1, "Ada")))
  }

  test("decodeAs reports TOON that does not fit the type as a Mapping error") {
    ToonTyped.decodeAs[User]("id: one") match {
    case Left(_: DecodeError.Mapping) => ()
    case other                        => fail(s"expected a Mapping error, got $other")
    }
  }

}
