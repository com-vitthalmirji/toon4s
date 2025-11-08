package io.toonformat.toon4s

import io.toonformat.toon4s.codec.{Decoder, Encoder}
import io.toonformat.toon4s.error.DecodeError

object syntax {

  implicit final class EncodeOps[A](private val a: A) extends AnyVal {

    def toJsonValue(implicit enc: Encoder[A]): JsonValue = enc(a)

    def toToon(options: EncodeOptions = EncodeOptions())(implicit enc: Encoder[A]): String =
      io.toonformat.toon4s.encode.Encoders.encode(enc(a), options)

  }

  implicit final class JsonValueOps(private val j: JsonValue) extends AnyVal {

    def as[A](implicit dec: Decoder[A]): Either[DecodeError, A] = dec(j)

  }

}
