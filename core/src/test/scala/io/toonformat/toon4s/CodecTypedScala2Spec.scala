package io.toonformat.toon4s

import io.toonformat.toon4s.JsonValue._
import io.toonformat.toon4s.codec.{Decoder, Encoder}
import munit.FunSuite

class CodecTypedScala2Spec extends FunSuite {

  test("Scala 2 typed API: manual instances encode/decode") {
    case class User(id: Int, name: String)

    implicit val userEnc: Encoder[User] = (u: User) =>
      JObj(scala.collection.immutable.VectorMap("id" -> JNumber(u.id), "name" -> JString(u.name)))

    implicit val userDec: Decoder[User] = {
      case JObj(m) =>
        for {
          id <- implicitly[Decoder[Int]].apply(m.getOrElse("id", JNull))
          name <- implicitly[Decoder[String]].apply(m.getOrElse("name", JNull))
        } yield User(id, name)
      case other =>
        Left(io.toonformat.toon4s.error.DecodeError.Mapping(s"Expected object, found $other"))
    }

    val u = User(1, "Ada")
    val toon = ToonTyped.encode(u).fold(throw _, identity)
    assert(!toon.contains("users")) // simple smoke
    val json = Toon.decode(toon).fold(throw _, identity)
    val u2 = ToonTyped.decodeAs[User](toon).fold(throw _, identity)
    assertEquals(u2, u)
  }

}
