package io.toonformat.toon4s
package decode
package parsers

import io.toonformat.toon4s.{Constants => C}
import io.toonformat.toon4s.JsonValue

/**
 * Parser for primitive values (strings, numbers, booleans, null).
 *
 * ==Design: Single responsibility principle + Pattern matching==
 *
 * This object handles only primitive value parsing:
 *   - Detecting value types (numeric, boolean, null, string)
 *   - Converting strings to appropriate JsonValue types
 *   - Handling quoted vs unquoted strings
 *
 * ==Type detection plan==
 *   1. Empty/whitespace → empty string 2. Starts with `"` → quoted string 3. Matches boolean/null
 *      literals → JBool/JNull 4. Matches numeric pattern → JNumber 5. Everything else → unquoted
 *      string
 *
 * @example
 *   {{{
 * PrimitiveParser.parsePrimitiveToken("42")          // JNumber(42)
 * PrimitiveParser.parsePrimitiveToken("true")        // JBool(true)
 * PrimitiveParser.parsePrimitiveToken("\"hello\"")   // JString("hello")
 * PrimitiveParser.parsePrimitiveToken("null")        // JNull
 *   }}}
 */
object PrimitiveParser {

  /**
   * Parse a primitive token into a JsonValue.
   *
   * ==Pattern matching for type detection==
   * Uses guards and predicates to determine the appropriate type.
   *
   * @param token
   *   The token string to parse
   * @return
   *   The corresponding JsonValue (JString, JNumber, JBool, or JNull)
   *
   * @example
   *   {{{
   * parsePrimitiveToken("123")       // JNumber(BigDecimal(123))
   * parsePrimitiveToken("3.14")      // JNumber(BigDecimal(3.14))
   * parsePrimitiveToken("true")      // JBool(true)
   * parsePrimitiveToken("false")     // JBool(false)
   * parsePrimitiveToken("null")      // JNull
   * parsePrimitiveToken("\"text\"")  // JString("text")
   * parsePrimitiveToken("text")      // JString("text")
   *   }}}
   */
  def parsePrimitiveToken(token: String): JsonValue = {
    val trimmed = token.trim

    // Empty string case
    if (trimmed.isEmpty) JsonValue.JString("")

    // Quoted string case
    else if (trimmed.headOption.contains('"'))
      JsonValue.JString(StringLiteralParser.parseStringLiteral(trimmed))

    // Boolean/null literal case
    else if (isBooleanOrNullLiteral(trimmed))
      trimmed match {
      case C.TrueLiteral  => JsonValue.JBool(true)
      case C.FalseLiteral => JsonValue.JBool(false)
      case _              => JsonValue.JNull
      }

    // Numeric case
    else if (isNumericLiteral(trimmed)) {
      val bd = BigDecimal(trimmed)
      // Normalize -0 to 0 for canonical representation
      val norm = if (bd == BigDecimal(0) && trimmed.startsWith("-")) BigDecimal(0) else bd
      JsonValue.JNumber(norm)
    }

    // Default: unquoted string
    else JsonValue.JString(trimmed)
  }

  /**
   * Check if a value is a boolean or null literal.
   *
   * ==Pure predicate function==
   * Simple equality check, no side effects.
   *
   * @param value
   *   The string value to check
   * @return
   *   true if the value is "true", "false", or "null"
   *
   * @example
   *   {{{
   * isBooleanOrNullLiteral("true")   // true
   * isBooleanOrNullLiteral("false")  // true
   * isBooleanOrNullLiteral("null")   // true
   * isBooleanOrNullLiteral("123")    // false
   *   }}}
   */
  private def isBooleanOrNullLiteral(value: String): Boolean =
    value == C.TrueLiteral || value == C.FalseLiteral || value == C.NullLiteral

  /**
   * Regex pattern for numeric literals.
   *
   * Matches:
   *   - Optional negative sign
   *   - Integer part (one or more digits)
   *   - Optional decimal part (dot followed by digits)
   *   - Optional exponent (e/E followed by optional sign and digits)
   *
   * Examples: `42`, `-3.14`, `1.23e10`, `-5.67E-8`
   */
  private val NumericPattern = "^-?[0-9]+(?:\\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$".r

  /**
   * Check if a token represents a valid numeric literal.
   *
   * ==Validation plan==
   *   1. Reject empty strings 2. Reject leading zeros (except "0" and "0.xxx") 3. Match against
   *      regex pattern
   *
   * This prevents invalid numbers like "007" while allowing "0.7".
   *
   * @param token
   *   The string token to validate
   * @return
   *   true if the token is a valid numeric literal
   *
   * @example
   *   {{{
   * isNumericLiteral("42")       // true
   * isNumericLiteral("-3.14")    // true
   * isNumericLiteral("1.23e10")  // true
   * isNumericLiteral("007")      // false (leading zero)
   * isNumericLiteral("0.7")      // true (valid decimal)
   * isNumericLiteral("abc")      // false
   *   }}}
   */
  private def isNumericLiteral(token: String): Boolean = {
    if (token.isEmpty) false
    else {
      val unsigned = if (token.head == '-' || token.head == '+') token.tail else token
      val hasLeadingZero = unsigned.length > 1 && unsigned.head == '0' && unsigned(1) != '.'
      unsigned.nonEmpty && !hasLeadingZero && NumericPattern.matches(token)
    }
  }

}
