package io.toonformat.toon4s.error

object JsonError {
  final case class Parse(override val message: String)
      extends RuntimeException(message)
      with ToonError
}
