package io.toonformat.toon4s
package decode
package parsers

import io.toonformat.toon4s.{Constants => C}
import io.toonformat.toon4s.error.DecodeError

/**
 * Parser for object keys (quoted and unquoted).
 *
 * ==Design: Single responsibility principle==
 *
 * This object handles only key parsing concerns:
 *   - Parsing quoted keys with escape sequences
 *   - Parsing unquoted keys
 *   - Finding the colon separator
 *   - Returning both the key and the position after the colon
 *
 * ==Key Syntax==
 *   - Quoted: `"key name": value` - allows spaces and special characters
 *   - Unquoted: `keyname: value` - simple alphanumeric keys
 *
 * @example
 *   {{{
 * KeyParser.parseKeyToken("name: Alice", 0)
 * // Result: ("name", 6) - key is "name", next position is 6
 *
 * KeyParser.parseKeyToken("\"first name\": Alice", 0)
 * // Result: ("first name", 14) - quoted key with space
 *   }}}
 */
object KeyParser {

  /**
   * Parse a key token and return the key with the position after the colon.
   *
   * ==Higher-order function==
   * Delegates to specialized parsers based on first character.
   *
   * @param content
   *   The line content containing the key
   * @param start
   *   Starting position in the content
   * @return
   *   Tuple of (key, position after colon)
   * @throws io.toonformat.toon4s.error.DecodeError.Syntax
   *   if key is malformed or colon is missing
   *
   * @example
   *   {{{
   * parseKeyToken("age: 30", 0)  // ("age", 5)
   * parseKeyToken("\"name\": Alice", 0)  // ("name", 8)
   *   }}}
   */
  def parseKeyToken(content: String, start: Int): (String, Int) =
    if (content.charAt(start) == '"') parseQuotedKey(content, start)
    else parseUnquotedKey(content, start)

  /**
   * Parse an unquoted key (stops at colon).
   *
   * ==Pure function==
   * No side effects, deterministic output.
   *
   * @param content
   *   The line content
   * @param start
   *   Starting position
   * @return
   *   Tuple of (key, position after colon)
   * @throws io.toonformat.toon4s.error.DecodeError.Syntax
   *   if colon is not found
   *
   * @example
   *   {{{
   * parseUnquotedKey("name: Alice", 0)  // ("name", 6)
   * parseUnquotedKey("age:30", 0)       // ("age", 4)
   *   }}}
   */
  def parseUnquotedKey(content: String, start: Int): (String, Int) = {
    var end = start
    while (end < content.length && content.charAt(end) != C.Colon) end += 1
    if (end >= content.length || content.charAt(end) != C.Colon)
      throw DecodeError.Syntax("Missing colon after key")
    val key = content.substring(start, end).trim
    (key, end + 1)
  }

  /**
   * Parse a quoted key with escape sequences.
   *
   * ==Composition pattern==
   * Uses StringLiteralParser for the actual unescaping.
   *
   * @param content
   *   The line content
   * @param start
   *   Starting position (should be at opening quote)
   * @return
   *   Tuple of (key, position after colon)
   * @throws io.toonformat.toon4s.error.DecodeError.Syntax
   *   if quote is unterminated or colon is missing
   *
   * @example
   *   {{{
   * parseQuotedKey("\"first name\": Alice", 0)  // ("first name", 14)
   * parseQuotedKey("\"key\\twith\\ttabs\": value", 0)  // ("key\twith\ttabs", ...)
   *   }}}
   */
  def parseQuotedKey(content: String, start: Int): (String, Int) = {
    val closing = StringLiteralParser.findClosingQuote(content, start)
    if (closing < 0) throw DecodeError.Syntax("Unterminated quoted key")
    if (closing + 1 >= content.length || content.charAt(closing + 1) != C.Colon)
      throw DecodeError.Syntax("Missing colon after key")
    val key = StringLiteralParser.unescapeString(content.substring(start + 1, closing))
    (key, closing + 2)
  }

}
