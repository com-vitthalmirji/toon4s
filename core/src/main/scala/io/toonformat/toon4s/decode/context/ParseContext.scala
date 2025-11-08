package io.toonformat.toon4s
package decode
package context

import io.toonformat.toon4s.error.{DecodeError, ErrorLocation}

/**
 * Parse context for error tracking.
 *
 * ==Design: Reader Monad + Error Context Pattern==
 *
 * This case class encapsulates location information that flows through parsing operations, enabling
 * automatic error location tracking without threading location through every function.
 *
 * ==Strategy==
 * Instead of modifying every parser function signature to accept and return location, we provide
 * helper methods that catch exceptions and enrich them with location context.
 *
 * @param line
 *   Current line number (1-based)
 * @param column
 *   Current column number (1-based)
 * @param content
 *   Current line content for error snippets
 *
 * @example
 *   {{{
 * val ctx = ParseContext(lineNumber = 5, column = 12, content = "key: value")
 *
 * // Automatic location enrichment on errors
 * ctx.withLocation {
 *   Parser.parseKeyToken(content, 0)
 * }
 * // If parseKeyToken throws DecodeError without location, it gets enriched with ctx location
 *   }}}
 */
final case class ParseContext(
    line: Int,
    column: Int,
    content: String,
) {

  /**
   * Execute a block and enrich any DecodeError with this context's location.
   *
   * ==Error Handling Pattern: Exception Enrichment==
   *
   * If the block throws a DecodeError that doesn't already have location information, this method
   * adds the location from this context.
   *
   * @param block
   *   The parsing operation to execute
   * @return
   *   Result of the block
   * @throws DecodeError
   *   with location information if block throws DecodeError
   *
   * @example
   *   {{{
   * val ctx = ParseContext(5, 12, "invalid: \"unterminated)
   * ctx.withLocation {
   *   StringLiteralParser.parseStringLiteral("\"unterminated")
   * }
   * // Throws: DecodeError.Syntax("Unterminated...", Some(ErrorLocation(5, 12, ...)))
   *   }}}
   */
  def withLocation[A](block: => A): A = {
    try {
      block
    } catch {
      case e: DecodeError.Syntax if e.location.isEmpty =>
        throw e.copy(location = Some(toErrorLocation))
      case e: DecodeError.Range if e.location.isEmpty =>
        throw e.copy(location = Some(toErrorLocation))
      case e: DecodeError.InvalidHeader if e.location.isEmpty =>
        throw e.copy(location = Some(toErrorLocation))
      case e: DecodeError.Mapping if e.location.isEmpty =>
        throw e.copy(location = Some(toErrorLocation))
      case e: DecodeError.Unexpected if e.location.isEmpty =>
        throw e.copy(location = Some(toErrorLocation))
      case other: Throwable => throw other
    }
  }

  /**
   * Execute a block and map any DecodeError with location enrichment.
   *
   * ==Functional Error Handling: Either + Error Enrichment==
   *
   * Similar to withLocation but returns Either instead of throwing.
   *
   * @param block
   *   The parsing operation to execute
   * @return
   *   Right(result) or Left(error with location)
   *
   * @example
   *   {{{
   * val ctx = ParseContext(5, 12, "arr[invalid]:")
   * ctx.catching {
   *   ArrayHeaderParser.parseArrayHeaderLine(ctx.content, Delimiter.Comma)
   * }
   * // Returns: Left(DecodeError.InvalidHeader(..., Some(ErrorLocation(5, 12, ...))))
   *   }}}
   */
  def catching[A](block: => A): Either[DecodeError, A] = {
    try {
      Right(block)
    } catch {
      case e: DecodeError.Syntax if e.location.isEmpty =>
        Left(e.copy(location = Some(toErrorLocation)))
      case e: DecodeError.Range if e.location.isEmpty =>
        Left(e.copy(location = Some(toErrorLocation)))
      case e: DecodeError.InvalidHeader if e.location.isEmpty =>
        Left(e.copy(location = Some(toErrorLocation)))
      case e: DecodeError.Mapping if e.location.isEmpty =>
        Left(e.copy(location = Some(toErrorLocation)))
      case e: DecodeError.Unexpected if e.location.isEmpty =>
        Left(e.copy(location = Some(toErrorLocation)))
      case e: DecodeError =>
        Left(e) // Already has location, preserve it
    }
  }

  /**
   * Create ErrorLocation from this context.
   *
   * ==Pure Function==
   *
   * @return
   *   ErrorLocation with this context's information
   */
  def toErrorLocation: ErrorLocation =
    ErrorLocation(line, column, content)

  /**
   * Create new context with updated column.
   *
   * ==Immutable Update Pattern==
   *
   * @param newColumn
   *   New column number
   * @return
   *   New context with updated column
   *
   * @example
   *   {{{
   * val ctx = ParseContext(5, 0, "key: value")
   * val afterKey = ctx.withColumn(5)  // ParseContext(5, 5, "key: value")
   *   }}}
   */
  def withColumn(newColumn: Int): ParseContext =
    copy(column = newColumn)

  /**
   * Create new context from a parsed line.
   *
   * ==Factory Method Pattern==
   *
   * @param newLine
   *   New line number
   * @param newContent
   *   New line content
   * @return
   *   New context for the new line
   */
  def withLine(newLine: Int, newContent: String): ParseContext =
    ParseContext(newLine, column = 1, newContent)

}

object ParseContext {

  /**
   * Create context from a ParsedLine.
   *
   * ==Factory Method Pattern==
   *
   * @param parsedLine
   *   The parsed line to create context from
   * @return
   *   ParseContext with line information
   *
   * @example
   *   {{{
   * val pl = ParsedLine(lineNumber = 5, depth = 0, content = "key: value")
   * val ctx = ParseContext.fromParsedLine(pl)
   * // ParseContext(5, 1, "key: value")
   *   }}}
   */
  def fromParsedLine(parsedLine: ParsedLine): ParseContext =
    ParseContext(
      line = parsedLine.lineNumber,
      column = 1,
      content = parsedLine.content,
    )

  /**
   * Create context with line number and content.
   *
   * ==Convenience Constructor==
   *
   * @param line
   *   Line number (1-based)
   * @param content
   *   Line content
   * @return
   *   ParseContext with column = 1
   */
  def apply(line: Int, content: String): ParseContext =
    ParseContext(line, column = 1, content)

}
