package io.toonformat.toon4s.error

sealed trait ToonError extends RuntimeException {
  def message: String
  override def getMessage: String = message
}
