package io.toonformat.toon4s.error

/**
 * Location information for parse errors.
 *
 * @param line
 *   Line number (1-based)
 * @param column
 *   Column number (1-based)
 * @param snippet
 *   The problematic line or relevant context
 *
 * @example
 *   {{{
 * ErrorLocation(5, 12, "key value")  // Missing colon at line 5, column 12
 *   }}}
 */
final case class ErrorLocation(line: Int, column: Int, snippet: String) {

  override def toString: String = s"$line:$column"

}

/**
 * Base trait for all TOON format errors.
 *
 * All errors extend [[java.lang.RuntimeException RuntimeException]] for Java interoperability and
 * can be caught as standard exceptions.
 *
 * @see
 *   [[EncodeError]] for encoding failures
 * @see
 *   [[DecodeError]] for decoding failures
 */
sealed trait ToonError extends RuntimeException {

  def message: String

  override def getMessage: String = message

}

/**
 * Errors that occur during TOON encoding.
 *
 * @see
 *   [[EncodeError.Normalization]] for value normalization failures
 */
sealed trait EncodeError extends ToonError

object EncodeError {

  /**
   * Failed to normalize a Scala value to JsonValue.
   *
   * This occurs when encoding unsupported types (e.g., functions, threads, custom classes without
   * proper conversion).
   *
   * @param message
   *   Error description
   *
   * @example
   *   {{{
   * Toon.encode(List(() => 42)) // Left(Normalization("Cannot encode function..."))
   *   }}}
   */
  final case class Normalization(override val message: String)
      extends RuntimeException(message)
      with EncodeError

}

/**
 * Errors that occur during TOON decoding.
 *
 * Different error types help distinguish between syntax errors, out-of-range values, invalid
 * headers, and unexpected failures.
 *
 * All decode errors can optionally include location information (line, column, snippet) to help
 * pinpoint the source of the error in the input.
 *
 * @see
 *   [[DecodeError.Syntax]] for syntax violations
 * @see
 *   [[DecodeError.Range]] for out-of-range values
 * @see
 *   [[DecodeError.InvalidHeader]] for malformed array headers
 * @see
 *   [[DecodeError.Mapping]] for structure mapping issues
 * @see
 *   [[DecodeError.Unexpected]] for unexpected errors
 */
sealed trait DecodeError extends ToonError {

  def location: Option[ErrorLocation]

}

object DecodeError {

  /**
   * Syntax error in TOON input.
   *
   * Includes: invalid escape sequences, unterminated strings, malformed structures, blank lines in
   * arrays.
   *
   * @param message
   *   Error description
   * @param location
   *   Optional location information (line, column, snippet)
   *
   * @example
   *   {{{
   * // Without location
   * Toon.decode("text: \"unterminated") // Left(Syntax("Unterminated string", None))
   *
   * // With location
   * // Error message: "5:12: Invalid escape sequence\n  text: \"\\x\""
   * DecodeError.Syntax("Invalid escape sequence", Some(ErrorLocation(5, 12, "text: \"\\x\"")))
   *   }}}
   */
  final case class Syntax(
      override val message: String,
      override val location: Option[ErrorLocation] = None,
  ) extends RuntimeException(message)
      with DecodeError {

    override def getMessage: String = location match {
    case Some(loc) => s"${loc.line}:${loc.column}: $message\n  ${loc.snippet}"
    case None      => message
    }

  }

  /**
   * Value out of allowed range.
   *
   * Includes: array length mismatch, exceeded depth limits, exceeded size limits, count mismatches.
   *
   * @param message
   *   Error description
   * @param location
   *   Optional location information (line, column, snippet)
   *
   * @example
   *   {{{
   * Toon.decode("arr[5]: 1,2,3") // Left(Range("Expected 5 items, got 3", None))
   * val opts = DecodeOptions(maxDepth = Some(3))
   * Toon.decode(deeplyNested, opts) // Left(Range("Exceeded maximum nesting depth", Some(...)))
   *   }}}
   */
  final case class Range(
      override val message: String,
      override val location: Option[ErrorLocation] = None,
  ) extends RuntimeException(message)
      with DecodeError {

    override def getMessage: String = location match {
    case Some(loc) => s"${loc.line}:${loc.column}: $message\n  ${loc.snippet}"
    case None      => message
    }

  }

  /**
   * Invalid array header format.
   *
   * Occurs when array headers (e.g., `arr[N]`, `arr[N]{fields}`) are malformed.
   *
   * @param message
   *   Error description
   * @param location
   *   Optional location information (line, column, snippet)
   *
   * @example
   *   {{{
   * Toon.decode("arr[invalid]: 1,2,3") // Left(InvalidHeader("Invalid array length", None))
   * Toon.decode("arr{}: 1,2") // Left(InvalidHeader("Invalid array header", None))
   *   }}}
   */
  final case class InvalidHeader(
      override val message: String,
      override val location: Option[ErrorLocation] = None,
  ) extends RuntimeException(message)
      with DecodeError {

    override def getMessage: String = location match {
    case Some(loc) => s"${loc.line}:${loc.column}: $message\n  ${loc.snippet}"
    case None      => message
    }

  }

  /**
   * Structure mapping error.
   *
   * Occurs when tabular data rows don't match schema or object fields are missing.
   *
   * @param message
   *   Error description
   * @param location
   *   Optional location information (line, column, snippet)
   *
   * @example
   *   {{{
   * // Tabular array with wrong number of columns
   * Toon.decode("""
   *   users[2]{id,name}:
   *     1,Alice,extra
   * """) // Left(Mapping("Expected 2 fields, got 3", Some(...)))
   *   }}}
   */
  final case class Mapping(
      override val message: String,
      override val location: Option[ErrorLocation] = None,
  ) extends RuntimeException(message)
      with DecodeError {

    override def getMessage: String = location match {
    case Some(loc) => s"${loc.line}:${loc.column}: $message\n  ${loc.snippet}"
    case None      => message
    }

  }

  /**
   * Unexpected error during decoding.
   *
   * Catch-all for errors that don't fit other categories (e.g., I/O errors, unexpected exceptions).
   *
   * @param message
   *   Error description
   * @param location
   *   Optional location information (line, column, snippet)
   */
  final case class Unexpected(
      override val message: String,
      override val location: Option[ErrorLocation] = None,
  ) extends RuntimeException(message)
      with DecodeError {

    override def getMessage: String = location match {
    case Some(loc) => s"${loc.line}:${loc.column}: $message\n  ${loc.snippet}"
    case None      => message
    }

  }

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
