package io.toonformat.toon4s.error

object DecodeError {
  final case class Syntax(override val message: String)
      extends RuntimeException(message)
      with DecodeError

  final case class Range(override val message: String)
      extends RuntimeException(message)
      with DecodeError

  final case class InvalidHeader(override val message: String)
      extends RuntimeException(message)
      with DecodeError

  final case class Unexpected(override val message: String)
      extends RuntimeException(message)
      with DecodeError
}

sealed trait DecodeError extends ToonError
