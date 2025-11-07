package io.toonformat.toon4s.error

sealed trait EncodeError extends ToonError

object EncodeError {
  final case class Normalization(override val message: String)
      extends RuntimeException(message)
      with EncodeError
}
