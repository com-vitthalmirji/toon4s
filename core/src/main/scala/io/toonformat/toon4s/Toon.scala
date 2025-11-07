package io.toonformat.toon4s

import io.toonformat.toon4s.encode.Encoders
import io.toonformat.toon4s.decode.Decoders
import io.toonformat.toon4s.error.{DecodeError, EncodeError}
import io.toonformat.toon4s.util.EitherUtils._

import scala.util.Try

/** Main entry point for the TOON (Token-Oriented Object Notation) format.
  *
  * TOON is a human-readable data serialization format designed for structured data, offering better
  * readability than JSON while maintaining similar expressiveness.
  *
  * ==Basic Usage==
  * {{{
  * import io.toonformat.toon4s._
  * import io.toonformat.toon4s.JsonValue._
  *
  * // Encoding
  * val data = JObj(VectorMap("name" -> JString("Alice"), "age" -> JNumber(30)))
  * val toon = Toon.encode(data)
  * // Result: Right("name: Alice\nage: 30")
  *
  * // Decoding
  * val parsed = Toon.decode("name: Alice\nage: 30")
  * // Result: Right(JObj(VectorMap("name" -> JString("Alice"), "age" -> JNumber(30))))
  * }}}
  *
  * ==Custom Options==
  * {{{
  * // Use tab delimiter
  * val options = EncodeOptions(delimiter = Delimiter.Tab)
  * Toon.encode(data, options)
  *
  * // Lenient decoding
  * val decodeOpts = DecodeOptions(strictness = Strictness.Lenient)
  * Toon.decode(input, decodeOpts)
  * }}}
  *
  * @see
  *   [[EncodeOptions]] for encoding configuration
  * @see
  *   [[DecodeOptions]] for decoding configuration
  * @see
  *   [[JsonValue]] for the data model
  */
object Toon {

  /** Encodes a value to TOON format.
    *
    * Accepts Scala values (Maps, Lists, primitives) or [[JsonValue]] instances and converts them to
    * TOON format.
    *
    * @param value
    *   The value to encode. Can be a JsonValue or native Scala type (Map, List, String, Number,
    *   Boolean, null)
    * @param options
    *   Encoding options (indent size, delimiter choice)
    * @return
    *   Right(toon) on success, Left(error) on failure
    *
    * @example
    *   {{{
    * val data = Map("name" -> "Alice", "age" -> 30)
    * Toon.encode(data) // Right("name: Alice\nage: 30")
    *   }}}
    */
  def encode(value: Any, options: EncodeOptions = EncodeOptions()): EncodeResult[String] =
    for {
      normalized <- Try(internal.Normalize.toJson(value)).toEitherMap(
                      t => EncodeError.Normalization(t.getMessage)
                    )
      out        <- Try(Encoders.encode(normalized, options)).toEitherMap(
                      t => EncodeError.Normalization(t.getMessage)
                    )
    } yield out

  /** Encodes a value to TOON format and writes to a Writer.
    *
    * Useful for streaming large outputs directly to files or network sockets without building the
    * entire string in memory.
    *
    * @param value
    *   The value to encode
    * @param out
    *   The Writer to write TOON output to
    * @param options
    *   Encoding options
    * @return
    *   Right(()) on success, Left(error) on failure
    *
    * @example
    *   {{{
    * val writer = new java.io.FileWriter("output.toon")
    * Toon.encodeTo(data, writer)
    * writer.close()
    *   }}}
    */
  def encodeTo(
      value: Any,
      out: java.io.Writer,
      options: EncodeOptions = EncodeOptions()
  ): EncodeResult[Unit] =
    for {
      normalized <- Try(internal.Normalize.toJson(value)).toEitherMap(
                      t => EncodeError.Normalization(t.getMessage)
                    )
      _          <- Try({ Encoders.encodeTo(normalized, out, options); out.flush() }).toEitherMap(
                      t => EncodeError.Normalization(t.getMessage)
                    )
    } yield ()

  /** Decodes a TOON string to a JsonValue.
    *
    * Parses TOON format and returns the structured data as a [[JsonValue]] tree.
    *
    * @param input
    *   The TOON string to decode
    * @param options
    *   Decoding options (strictness level, size limits)
    * @return
    *   Right(jsonValue) on success, Left(error) on parse failure
    *
    * @example
    *   {{{
    * val toon = "name: Alice\nage: 30"
    * Toon.decode(toon) match {
    *   case Right(value) => println(value)
    *   case Left(error) => println(s"Parse error: \${error.message}")
    * }
    *   }}}
    */
  def decode(input: String, options: DecodeOptions = DecodeOptions()): DecodeResult[JsonValue] =
    Try(Decoders.decode(input, options)).toEither.left.map {
      case err: DecodeError => err
      case other            => DecodeError.Unexpected(other.getMessage)
    }

  /** Encodes a value to TOON format, throwing an exception on error.
    *
    * Convenience method that throws instead of returning Either. Use only when you're certain the
    * input is valid.
    *
    * @param value
    *   The value to encode
    * @param options
    *   Encoding options
    * @return
    *   The TOON string
    * @throws EncodeError
    *   if encoding fails
    *
    * @example
    *   {{{
    * val toon = Toon.encodeUnsafe(Map("key" -> "value"))
    *   }}}
    */
  def encodeUnsafe(value: Any, options: EncodeOptions = EncodeOptions()): String =
    encode(value, options).fold(throw _, identity)

  /** Decodes a TOON string to JsonValue, throwing an exception on error.
    *
    * Convenience method that throws instead of returning Either. Use only when you're certain the
    * input is valid.
    *
    * @param input
    *   The TOON string to decode
    * @param options
    *   Decoding options
    * @return
    *   The decoded JsonValue
    * @throws DecodeError
    *   if decoding fails
    *
    * @example
    *   {{{
    * val value = Toon.decodeUnsafe("name: Alice")
    *   }}}
    */
  def decodeUnsafe(input: String, options: DecodeOptions = DecodeOptions()): JsonValue =
    decode(input, options).fold(throw _, identity)

  /** Decodes TOON from a Reader.
    *
    * Reads TOON content from a Reader (file, network stream, etc.) and parses it.
    *
    * '''Note:''' This method reads the entire input into memory before parsing. For large files
    * (>100MB), consider memory constraints. See P1.9 documentation for streaming limitations.
    *
    * @param in
    *   The Reader to read TOON content from
    * @param options
    *   Decoding options
    * @return
    *   Right(jsonValue) on success, Left(error) on failure
    *
    * @example
    *   {{{
    * val reader = new java.io.FileReader("data.toon")
    * val result = Toon.decodeFrom(reader)
    * reader.close()
    *   }}}
    */
  def decodeFrom(
      in: java.io.Reader,
      options: DecodeOptions = DecodeOptions()
  ): DecodeResult[JsonValue] =
    Try {
      val isStrict = options.strictness == Strictness.Strict
      val scan     = io.toonformat.toon4s.decode.Scanner.toParsedLines(in, options.indent, isStrict)
      Decoders.decodeScan(scan, options)
    }.toEither.left.map {
      case err: DecodeError => err
      case other            => DecodeError.Unexpected(other.getMessage)
    }
}
