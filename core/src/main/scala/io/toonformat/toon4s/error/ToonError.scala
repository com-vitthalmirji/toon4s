package io.toonformat.toon4s.error

sealed trait ToonError extends RuntimeException {
  def message: String
  override def getMessage: String = message
}

sealed trait EncodeError extends ToonError

object EncodeError {
  final case class Normalization(override val message: String)
      extends RuntimeException(message)
      with EncodeError
}

sealed trait DecodeError extends ToonError

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

sealed trait JsonError extends ToonError

object JsonError {
  final case class Parse(override val message: String)
      extends RuntimeException(message)
      with JsonError
}

sealed trait IoError extends ToonError

object IoError {
  final case class File(override val message: String, cause: Throwable)
      extends RuntimeException(message, cause)
      with IoError
}
