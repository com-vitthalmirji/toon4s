package io.toonformat.toon4s

import io.toonformat.toon4s.codec.{Decoder, Encoder}

object ToonTyped {

  def encode[A](value: A, options: EncodeOptions = EncodeOptions())(implicit
      enc: Encoder[A]): EncodeResult[String] = {
    val json = enc.apply(value)
    Right(io.toonformat.toon4s.encode.Encoders.encode(json, options))
  }

  def encodeTo[A](value: A, out: java.io.Writer, options: EncodeOptions = EncodeOptions())(implicit
      enc: Encoder[A]): EncodeResult[Unit] = {
    val json = enc.apply(value)
    Right { io.toonformat.toon4s.encode.Encoders.encodeTo(json, out, options); out.flush() }
  }

  def decodeAs[A](input: String, options: DecodeOptions = DecodeOptions())(implicit
      dec: Decoder[A]): DecodeResult[A] =
    Toon.decode(input, options).flatMap(dec.apply)

}
