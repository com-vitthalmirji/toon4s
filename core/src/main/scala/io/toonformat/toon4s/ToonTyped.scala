package io.toonformat.toon4s

import io.toonformat.toon4s.codec.{Decoder, Encoder}

/**
 * Typed entry points: encode and decode through an [[codec.Encoder]] or [[codec.Decoder]] instead
 * of `Any`, so a value the codec cannot handle is a compile error rather than a runtime surprise.
 *
 * In Scala 3, case classes derive both codecs (`derives Encoder, Decoder`). In Scala 2.13, provide
 * them as implicit values.
 */
object ToonTyped {

  /**
   * Encodes a value to a TOON string with its [[codec.Encoder]].
   *
   * @param value
   *   The value to encode
   * @param options
   *   Encoding options (indent size, delimiter, key folding)
   * @return
   *   Right(toon) on success; the encoder runs before any TOON is written, so encoding a typed
   *   value does not fail once its encoder exists
   *
   * @example
   *   {{{
   * case class User(id: Int, name: String) derives Encoder
   *
   * ToonTyped.encode(User(1, "Ada")) // Right("id: 1\nname: Ada")
   *   }}}
   */
  def encode[A](value: A, options: EncodeOptions = EncodeOptions())(implicit
      enc: Encoder[A]): EncodeResult[String] = {
    val json = enc.apply(value)
    Right(io.toonformat.toon4s.encode.Encoders.encode(json, options))
  }

  /**
   * Encodes a value with its [[codec.Encoder]] and writes the TOON to `out`, then flushes it. The
   * writer is not closed.
   *
   * @param value
   *   The value to encode
   * @param out
   *   Where the TOON is written
   * @param options
   *   Encoding options (indent size, delimiter, key folding)
   * @return
   *   Right(()) once the output is written and flushed
   *
   * @example
   *   {{{
   * case class User(id: Int, name: String) derives Encoder
   *
   * val out = new java.io.StringWriter()
   * ToonTyped.encodeTo(User(1, "Ada"), out) // Right(())
   * out.toString                            // "id: 1\nname: Ada"
   *   }}}
   */
  def encodeTo[A](value: A, out: java.io.Writer, options: EncodeOptions = EncodeOptions())(implicit
      enc: Encoder[A]): EncodeResult[Unit] = {
    val json = enc.apply(value)
    Right { io.toonformat.toon4s.encode.Encoders.encodeTo(json, out, options); out.flush() }
  }

  /**
   * Decodes TOON and converts the result with a [[codec.Decoder]].
   *
   * @param input
   *   The TOON text
   * @param options
   *   Decoding options (indent size, strictness, path expansion)
   * @return
   *   Right(value), or Left with a [[error.DecodeError]]: a syntax error from the TOON itself, or a
   *   `DecodeError.Mapping` when the TOON parses but does not fit `A`
   *
   * @example
   *   {{{
   * case class User(id: Int, name: String) derives Decoder
   *
   * ToonTyped.decodeAs[User]("id: 1\nname: Ada") // Right(User(1, "Ada"))
   * ToonTyped.decodeAs[User]("id: one")           // Left(DecodeError.Mapping(...))
   *   }}}
   */
  def decodeAs[A](input: String, options: DecodeOptions = DecodeOptions())(implicit
      dec: Decoder[A]): DecodeResult[A] =
    Toon.decode(input, options).flatMap(dec.apply)

}
