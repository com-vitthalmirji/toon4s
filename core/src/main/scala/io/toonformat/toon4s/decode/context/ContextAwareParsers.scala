package io.toonformat.toon4s
package decode
package context

import io.toonformat.toon4s.{Delimiter, JsonValue}
import io.toonformat.toon4s.decode.parsers._

/**
 * Context-aware parsers that automatically track location information.
 *
 * ==Design: Decorator + Error context tracking==
 *
 * This object wraps the existing parsers with context-aware versions that automatically enrich
 * errors with location information. This provides backward compatibility while enabling better
 * error reporting.
 *
 * ==Plan==
 * Instead of modifying all existing parsers, we provide wrapper methods that:
 *   1. Accept a ParseContext parameter 2. Delegate to the existing parser 3. Catch and enrich any
 *      DecodeErrors with location
 *
 * ==Usage pattern==
 * New code should use these context-aware parsers for better error messages. Existing code can
 * continue using the direct parsers without location context.
 *
 * @example
 *   {{{
 * val ctx = ParseContext(lineNumber = 5, column = 12, content = "key: value")
 *
 * // Context-aware parsing - errors get automatic location
 * val (key, nextPos) = ContextAwareParsers.parseKeyToken("name: Alice", 0)(ctx)
 * // If error occurs: DecodeError.Syntax("...", Some(ErrorLocation(5, 12, "key: value")))
 *
 * // Compare to context-free parsing:
 * val (key2, nextPos2) = KeyParser.parseKeyToken("name: Alice", 0)
 * // If error occurs: DecodeError.Syntax("...", None) - no location info
 *   }}}
 */
object ContextAwareParsers {

  // ========================================================================
  // Key parsing with context
  // ========================================================================

  /**
   * Parse a key token with automatic error location tracking.
   *
   * ==Decorator==
   * Wraps KeyParser.parseKeyToken with context enrichment.
   *
   * @param content
   *   The line content containing the key
   * @param start
   *   Starting position in the content
   * @param ctx
   *   Parse context for location tracking
   * @return
   *   Tuple of (key, position after colon)
   * @throws io.toonformat.toon4s.error.DecodeError.Syntax
   *   with location information if parsing fails
   *
   * @example
   *   {{{
   * val ctx = ParseContext(5, 1, "name: Alice")
   * val (key, pos) = parseKeyToken("name: Alice", 0)(ctx)
   * // Result: ("name", 6)
   * // If error: DecodeError.Syntax("Missing colon", Some(ErrorLocation(5, 1, "name: Alice")))
   *   }}}
   */
  def parseKeyToken(content: String, start: Int)(implicit ctx: ParseContext): (String, Int) = {
    val columnCtx = ctx.withColumn(start + 1)
    columnCtx.withLocation {
      KeyParser.parseKeyToken(content, start)
    }
  }

  /**
   * Parse an unquoted key with automatic error location tracking.
   *
   * @param content
   *   The line content
   * @param start
   *   Starting position
   * @param ctx
   *   Parse context for location tracking
   * @return
   *   Tuple of (key, position after colon)
   */
  def parseUnquotedKey(content: String, start: Int)(implicit ctx: ParseContext): (String, Int) = {
    val columnCtx = ctx.withColumn(start + 1)
    columnCtx.withLocation {
      KeyParser.parseUnquotedKey(content, start)
    }
  }

  /**
   * Parse a quoted key with automatic error location tracking.
   *
   * @param content
   *   The line content
   * @param start
   *   Starting position
   * @param ctx
   *   Parse context for location tracking
   * @return
   *   Tuple of (key, position after colon)
   */
  def parseQuotedKey(content: String, start: Int)(implicit ctx: ParseContext): (String, Int) = {
    val columnCtx = ctx.withColumn(start + 1)
    columnCtx.withLocation {
      KeyParser.parseQuotedKey(content, start)
    }
  }

  // ========================================================================
  // String literal parsing with context
  // ========================================================================

  /**
   * Parse a string literal with automatic error location tracking.
   *
   * @param token
   *   The token to parse
   * @param ctx
   *   Parse context for location tracking
   * @return
   *   The unescaped string value
   * @throws io.toonformat.toon4s.error.DecodeError.Syntax
   *   with location if parsing fails
   *
   * @example
   *   {{{
   * val ctx = ParseContext(10, 5, "text: \"hello\\nworld\"")
   * val result = parseStringLiteral("\"hello\\nworld\"")(ctx)
   * // Result: "hello\nworld"
   * // If error: DecodeError.Syntax("Invalid escape", Some(ErrorLocation(10, 5, ...)))
   *   }}}
   */
  def parseStringLiteral(token: String)(implicit ctx: ParseContext): String = {
    ctx.withLocation {
      StringLiteralParser.parseStringLiteral(token)
    }
  }

  /**
   * Unescape a string with automatic error location tracking.
   *
   * @param s
   *   The string with escape sequences
   * @param ctx
   *   Parse context for location tracking
   * @return
   *   The unescaped string
   */
  def unescapeString(s: String)(implicit ctx: ParseContext): String = {
    ctx.withLocation {
      StringLiteralParser.unescapeString(s)
    }
  }

  // ========================================================================
  // Primitive parsing with context
  // ========================================================================

  /**
   * Parse a primitive token with automatic error location tracking.
   *
   * @param token
   *   The token to parse
   * @param ctx
   *   Parse context for location tracking
   * @return
   *   JsonValue representing the primitive
   *
   * @example
   *   {{{
   * val ctx = ParseContext(7, 10, "age: 42")
   * val value = parsePrimitiveToken("42")(ctx)
   * // Result: JNumber(42)
   *   }}}
   */
  def parsePrimitiveToken(token: String)(implicit ctx: ParseContext): JsonValue = {
    ctx.withLocation {
      PrimitiveParser.parsePrimitiveToken(token)
    }
  }

  // ========================================================================
  // Array header parsing with context
  // ========================================================================

  /**
   * Parse an array header line with automatic error location tracking.
   *
   * @param content
   *   The line content to parse
   * @param defaultDelim
   *   Default delimiter if not specified
   * @param ctx
   *   Parse context for location tracking
   * @return
   *   Option of (header info, optional inline values)
   *
   * @example
   *   {{{
   * val ctx = ParseContext(15, 1, "users[2]{id,name}:")
   * val result = parseArrayHeaderLine("users[2]{id,name}:", Delimiter.Comma)(ctx)
   * // Some((ArrayHeaderInfo(...), None))
   * // If error: DecodeError.InvalidHeader("...", Some(ErrorLocation(15, 1, ...)))
   *   }}}
   */
  def parseArrayHeaderLine(
      content: String,
      defaultDelim: Delimiter,
  )(implicit ctx: ParseContext): Option[(ArrayHeaderInfo, Option[String])] = {
    ctx.withLocation {
      ArrayHeaderParser.parseArrayHeaderLine(content, defaultDelim)
    }
  }

  /**
   * Parse bracket segment with automatic error location tracking.
   *
   * @param seg
   *   The string inside brackets
   * @param defaultDelim
   *   Default delimiter
   * @param ctx
   *   Parse context for location tracking
   * @return
   *   Tuple of (length, delimiter, has marker flag)
   */
  def parseBracketSegment(
      seg: String,
      defaultDelim: Delimiter,
  )(implicit ctx: ParseContext): (Int, Delimiter, Boolean) = {
    ctx.withLocation {
      ArrayHeaderParser.parseBracketSegment(seg, defaultDelim)
    }
  }

  // ========================================================================
  // Delimited values parsing with context
  // ========================================================================

  /**
   * Parse delimited values with automatic error location tracking.
   *
   * @param input
   *   Input string with delimited values
   * @param delim
   *   Delimiter character
   * @param ctx
   *   Parse context for location tracking
   * @return
   *   Vector of parsed values
   *
   * @example
   *   {{{
   * val ctx = ParseContext(8, 1, "1,2,3")
   * val values = parseDelimitedValues("1,2,3", Delimiter.Comma)(ctx)
   * // Vector("1", "2", "3")
   *   }}}
   */
  def parseDelimitedValues(
      input: String,
      delim: Delimiter,
  )(implicit ctx: ParseContext): Vector[String] = {
    ctx.withLocation {
      DelimitedValuesParser.parseDelimitedValues(input, delim)
    }
  }

  /**
   * Map row values to primitives with automatic error location tracking.
   *
   * @param values
   *   Raw string values
   * @param ctx
   *   Parse context for location tracking
   * @return
   *   Vector of JsonValues
   */
  def mapRowValuesToPrimitives(
      values: Vector[String]
  )(implicit ctx: ParseContext): Vector[JsonValue] = {
    ctx.withLocation {
      DelimitedValuesParser.mapRowValuesToPrimitives(values)
    }
  }

  // ========================================================================
  // Either-based API for functional error handling
  // ========================================================================

  /**
   * Parse key token returning Either for functional composition.
   *
   * ==Functional Error Handling Pattern==
   *
   * @param content
   *   The line content containing the key
   * @param start
   *   Starting position in the content
   * @param ctx
   *   Parse context for location tracking
   * @return
   *   Right((key, position)) or Left(error with location)
   *
   * @example
   *   {{{
   * val ctx = ParseContext(5, 1, "name: Alice")
   * parseKeyTokenEither("name: Alice", 0)(ctx) match {
   *   case Right((key, pos)) => println(s"Key: \$key at position \$pos")
   *   case Left(error)       => println(s"Error: \${error.getMessage}")
   * }
   *   }}}
   */
  def parseKeyTokenEither(
      content: String,
      start: Int,
  )(implicit ctx: ParseContext): Either[error.DecodeError, (String, Int)] = {
    val columnCtx = ctx.withColumn(start + 1)
    columnCtx.catching {
      KeyParser.parseKeyToken(content, start)
    }
  }

  /**
   * Parse string literal returning Either for functional composition.
   *
   * @param token
   *   The token to parse
   * @param ctx
   *   Parse context for location tracking
   * @return
   *   Right(unescaped string) or Left(error with location)
   */
  def parseStringLiteralEither(
      token: String
  )(implicit ctx: ParseContext): Either[error.DecodeError, String] = {
    ctx.catching {
      StringLiteralParser.parseStringLiteral(token)
    }
  }

  /**
   * Parse primitive token returning Either for functional composition.
   *
   * @param token
   *   The token to parse
   * @param ctx
   *   Parse context for location tracking
   * @return
   *   Right(JsonValue) or Left(error with location)
   */
  def parsePrimitiveTokenEither(
      token: String
  )(implicit ctx: ParseContext): Either[error.DecodeError, JsonValue] = {
    ctx.catching {
      PrimitiveParser.parsePrimitiveToken(token)
    }
  }

  /**
   * Parse array header returning Either for functional composition.
   *
   * @param content
   *   The line content to parse
   * @param defaultDelim
   *   Default delimiter
   * @param ctx
   *   Parse context for location tracking
   * @return
   *   Right(Some(header)) or Right(None) or Left(error with location)
   */
  def parseArrayHeaderLineEither(
      content: String,
      defaultDelim: Delimiter,
  )(implicit
      ctx: ParseContext): Either[error.DecodeError, Option[(ArrayHeaderInfo, Option[String])]] = {
    ctx.catching {
      ArrayHeaderParser.parseArrayHeaderLine(content, defaultDelim)
    }
  }

}
