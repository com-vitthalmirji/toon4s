package io.toonformat.toon4s
package decode

import io.toonformat.toon4s.{Delimiter, JsonValue}
import io.toonformat.toon4s.decode.parsers._

/**
 * Facade for all parsing operations.
 *
 * ==Design Pattern: Facade Pattern==
 *
 * This object provides a unified interface to all specialized parsers, maintaining backward
 * compatibility while delegating to focused parser objects.
 *
 * ==Decomposition Strategy==
 * Parser responsibilities have been split into:
 *   - [[parsers.StringLiteralParser]] - String escaping/unescaping
 *   - [[parsers.KeyParser]] - Key parsing (quoted/unquoted)
 *   - [[parsers.PrimitiveParser]] - Primitive value detection
 *   - [[parsers.DelimitedValuesParser]] - CSV-style parsing
 *   - [[parsers.ArrayHeaderParser]] - Array header syntax
 *
 * This follows the Single Responsibility Principle (#27) while maintaining the existing API surface
 * for backward compatibility.
 *
 * @example
 *   {{{
 * import io.toonformat.toon4s.decode.Parser._
 *
 * val primitive = parsePrimitiveToken("42")  // JNumber(42)
 * val key = parseKeyToken("name: value", 0)  // ("name", 6)
 * val header = parseArrayHeaderLine("arr[3]: 1,2,3", Delimiter.Comma)
 *   }}}
 */
object Parser {

  // Re-export ArrayHeaderInfo for backward compatibility
  type ArrayHeaderInfo = parsers.ArrayHeaderInfo

  val ArrayHeaderInfo = parsers.ArrayHeaderInfo

  // ========================================================================
  // Array Header Parsing
  // ========================================================================

  /**
   * Parse an array header line.
   *
   * Delegates to [[parsers.ArrayHeaderParser]].
   *
   * @see
   *   [[parsers.ArrayHeaderParser.parseArrayHeaderLine]]
   */
  def parseArrayHeaderLine(
      content: String,
      defaultDelim: Delimiter,
  ): Option[(ArrayHeaderInfo, Option[String])] =
    ArrayHeaderParser.parseArrayHeaderLine(content, defaultDelim)

  /**
   * Parse bracket segment for array length and delimiter.
   *
   * Delegates to [[parsers.ArrayHeaderParser]].
   *
   * @see
   *   [[parsers.ArrayHeaderParser.parseBracketSegment]]
   */
  def parseBracketSegment(seg: String, defaultDelim: Delimiter): (Int, Delimiter, Boolean) =
    ArrayHeaderParser.parseBracketSegment(seg, defaultDelim)

  /**
   * Check if content is an array header after list hyphen.
   *
   * Delegates to [[parsers.ArrayHeaderParser]].
   *
   * @see
   *   [[parsers.ArrayHeaderParser.isArrayHeaderAfterHyphen]]
   */
  def isArrayHeaderAfterHyphen(content: String): Boolean =
    ArrayHeaderParser.isArrayHeaderAfterHyphen(content)

  /**
   * Check if content is an object field after list hyphen.
   *
   * Delegates to [[parsers.ArrayHeaderParser]].
   *
   * @see
   *   [[parsers.ArrayHeaderParser.isObjectFirstFieldAfterHyphen]]
   */
  def isObjectFirstFieldAfterHyphen(content: String): Boolean =
    ArrayHeaderParser.isObjectFirstFieldAfterHyphen(content)

  // ========================================================================
  // Delimited Values Parsing
  // ========================================================================

  /**
   * Parse delimited values with quote awareness.
   *
   * Delegates to [[parsers.DelimitedValuesParser]].
   *
   * @see
   *   [[parsers.DelimitedValuesParser.parseDelimitedValues]]
   */
  def parseDelimitedValues(input: String, delim: Delimiter): Vector[String] =
    DelimitedValuesParser.parseDelimitedValues(input, delim)

  /**
   * Map row values to primitive JsonValues.
   *
   * Delegates to [[parsers.DelimitedValuesParser]].
   *
   * @see
   *   [[parsers.DelimitedValuesParser.mapRowValuesToPrimitives]]
   */
  def mapRowValuesToPrimitives(values: Vector[String]): Vector[JsonValue] =
    DelimitedValuesParser.mapRowValuesToPrimitives(values)

  // ========================================================================
  // Primitive Value Parsing
  // ========================================================================

  /**
   * Parse a primitive token into a JsonValue.
   *
   * Delegates to [[parsers.PrimitiveParser]].
   *
   * @see
   *   [[parsers.PrimitiveParser.parsePrimitiveToken]]
   */
  def parsePrimitiveToken(token: String): JsonValue =
    PrimitiveParser.parsePrimitiveToken(token)

  // ========================================================================
  // String Literal Parsing
  // ========================================================================

  /**
   * Parse a string literal with escape sequences.
   *
   * Delegates to [[parsers.StringLiteralParser]].
   *
   * @see
   *   [[parsers.StringLiteralParser.parseStringLiteral]]
   */
  def parseStringLiteral(token: String): String =
    StringLiteralParser.parseStringLiteral(token)

  /**
   * Find closing quote in a string.
   *
   * Delegates to [[parsers.StringLiteralParser]].
   *
   * @see
   *   [[parsers.StringLiteralParser.findClosingQuote]]
   */
  def findClosingQuote(content: String, start: Int): Int =
    StringLiteralParser.findClosingQuote(content, start)

  /**
   * Unescape a string by converting escape sequences.
   *
   * Delegates to [[parsers.StringLiteralParser]].
   *
   * @see
   *   [[parsers.StringLiteralParser.unescapeString]]
   */
  def unescapeString(s: String): String =
    StringLiteralParser.unescapeString(s)

  /**
   * Find unquoted character in content.
   *
   * Delegates to [[parsers.StringLiteralParser]].
   *
   * @see
   *   [[parsers.StringLiteralParser.findUnquotedChar]]
   */
  def findUnquotedChar(content: String, target: Char): Int =
    StringLiteralParser.findUnquotedChar(content, target)

  // ========================================================================
  // Key Parsing
  // ========================================================================

  /**
   * Parse a key token (quoted or unquoted).
   *
   * Delegates to [[parsers.KeyParser]].
   *
   * @see
   *   [[parsers.KeyParser.parseKeyToken]]
   */
  def parseKeyToken(content: String, start: Int): (String, Int) =
    KeyParser.parseKeyToken(content, start)

  /**
   * Parse an unquoted key.
   *
   * Delegates to [[parsers.KeyParser]].
   *
   * @see
   *   [[parsers.KeyParser.parseUnquotedKey]]
   */
  def parseUnquotedKey(content: String, start: Int): (String, Int) =
    KeyParser.parseUnquotedKey(content, start)

  /**
   * Parse a quoted key.
   *
   * Delegates to [[parsers.KeyParser]].
   *
   * @see
   *   [[parsers.KeyParser.parseQuotedKey]]
   */
  def parseQuotedKey(content: String, start: Int): (String, Int) =
    KeyParser.parseQuotedKey(content, start)

}
