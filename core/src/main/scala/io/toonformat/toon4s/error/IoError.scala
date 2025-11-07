package io.toonformat.toon4s.error

object IoError {
  final case class File(override val message: String, cause: Throwable)
      extends RuntimeException(message, cause)
      with ToonError
}
