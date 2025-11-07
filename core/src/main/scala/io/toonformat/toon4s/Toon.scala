package io.toonformat.toon4s

import io.toonformat.toon4s.encode.Encoders
import io.toonformat.toon4s.decode.Decoders
import io.toonformat.toon4s.error.{DecodeError, EncodeError}

import scala.util.Try

object Toon {
  def encode(value: Any, options: EncodeOptions = EncodeOptions()): EncodeResult[String] =
    Try {
      val normalized = internal.Normalize.toJson(value)
      Encoders.encode(normalized, options)
    }.toEither.left.map {
      case err: EncodeError => err
      case other            => EncodeError.Normalization(other.getMessage)
    }

  def decode(input: String, options: DecodeOptions = DecodeOptions()): DecodeResult[JsonValue] =
    Try(Decoders.decode(input, options)).toEither.left.map {
      case err: DecodeError => err
      case other            => DecodeError.Unexpected(other.getMessage)
    }

  def encodeUnsafe(value: Any, options: EncodeOptions = EncodeOptions()): String =
    encode(value, options).fold(throw _, identity)

  def decodeUnsafe(input: String, options: DecodeOptions = DecodeOptions()): JsonValue =
    decode(input, options).fold(throw _, identity)
}
