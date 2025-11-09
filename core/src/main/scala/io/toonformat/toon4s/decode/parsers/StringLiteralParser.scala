package io.toonformat.toon4s
package decode
package parsers

import io.toonformat.toon4s.error.DecodeError

/**
 * Parser for quoted string literals with escape sequence handling.
 *
 * ==Design: Single responsibility principle==
 *
 * This object handles only string parsing concerns:
 *   - Finding closing quotes
 *   - Unescaping escape sequences
 *   - Validating string literal syntax
 *
 * ==Supported Escape Sequences==
 *   - `\"` → double quote
 *   - `\\` → backslash
 *   - `\n` → newline
 *   - `\r` → carriage return
 *   - `\t` → tab
 *
 * @example
 *   {{{
 * StringLiteralParser.parseStringLiteral("\"hello\\nworld\"")
 * // Result: "hello\nworld"
 *
 * StringLiteralParser.unescapeString("hello\\nworld")
 * // Result: "hello\nworld"
 *   }}}
 */
object StringLiteralParser {

  /**
   * Parse a string literal, handling both quoted and unquoted strings.
   *
   * Quoted strings must start and end with `"` and may contain escape sequences. Unquoted strings
   * are returned as-is after trimming.
   *
   * @param token
   *   The token to parse
   * @return
   *   The unescaped string value
   * @throws DecodeError#Syntax
   *   if quoted string is unterminated or has trailing content
   *
   * @example
   *   {{{
   * parseStringLiteral("\"hello\"")  // "hello"
   * parseStringLiteral("hello")      // "hello"
   * parseStringLiteral("\"un\\tterminated") // throws DecodeError.Syntax
   *   }}}
   */
  def parseStringLiteral(token: String): String = {
    val trimmed = token.trim
    trimmed.headOption.filter(_ == '"') match {
    case Some(_) =>
      val end = findClosingQuote(trimmed, 0)
      if (end < 0 || end != trimmed.length - 1)
        throw DecodeError.Syntax("Unterminated or trailing content after string literal")
      unescapeString(trimmed.slice(1, end))
    case None => trimmed
    }
  }

  /**
   * Find the position of the closing quote in a quoted string.
   *
   * Respects escape sequences - `\"` does not close the string.
   *
   * @param content
   *   The string content (must start with `"`)
   * @param start
   *   The starting position (index of opening quote)
   * @return
   *   Index of closing quote, or -1 if not found
   *
   * @example
   *   {{{
   * findClosingQuote("\"hello\"", 0)      // 6
   * findClosingQuote("\"he\\\"llo\"", 0)  // 8 (escaped quote doesn't close)
   *   }}}
   */
  def findClosingQuote(content: String, start: Int): Int = {
    var i = start + 1
    var closed = -1
    var escaped = false
    while (i < content.length && closed == -1) {
      val ch = content.charAt(i)
      if (escaped) escaped = false
      else if (ch == '\\') escaped = true
      else if (ch == '"') closed = i
      i += 1
    }
    closed
  }

  /**
   * Unescape a string by converting escape sequences to their actual characters.
   *
   * ==Pure function==
   * This function has no side effects and always produces the same output for the same input.
   *
   * @param s
   *   The string with escape sequences (without surrounding quotes)
   * @return
   *   The unescaped string
   * @throws DecodeError#Syntax
   *   if an invalid escape sequence is encountered
   *
   * @example
   *   {{{
   * unescapeString("hello\\nworld")  // "hello\nworld"
   * unescapeString("quote: \\\"")    // "quote: \""
   * unescapeString("invalid\\x")     // throws DecodeError.Syntax
   *   }}}
   */
  def unescapeString(s: String): String = {
    val builder = new StringBuilder
    var i = 0
    while (i < s.length) {
      s.charAt(i) match {
      case '\\' if i + 1 < s.length =>
        s.charAt(i + 1) match {
        case '"'   => builder.append('"')
        case '\\'  => builder.append('\\')
        case 'n'   => builder.append('\n')
        case 'r'   => builder.append('\r')
        case 't'   => builder.append('\t')
        case other =>
          throw DecodeError.Syntax(s"Invalid escape sequence: \\$other")
        }
        i += 2
      case '\\' =>
        throw DecodeError.Syntax("Unterminated escape sequence in string literal")
      case c =>
        builder.append(c)
        i += 1
      }
    }
    builder.result()
  }

  /**
   * Find the position of an unquoted character in a string.
   *
   * Skips over quoted sections when searching. Useful for finding delimiters that should only be
   * recognized outside of quotes.
   *
   * @param content
   *   The string to search
   * @param target
   *   The character to find
   * @return
   *   Index of the character, or -1 if not found outside quotes
   *
   * @example
   *   {{{
   * findUnquotedChar("key: value", ':')        // 3
   * findUnquotedChar("\"key:\" value", ':')    // -1 (colon is quoted)
   * findUnquotedChar("\"key\": value", ':')    // 5 (colon after quote)
   *   }}}
   */
  def findUnquotedChar(content: String, target: Char): Int = {
    var inQuotes = false
    var i = 0
    var result = -1
    while (i < content.length && result == -1) {
      val ch = content.charAt(i)
      if (ch == '\\' && inQuotes && i + 1 < content.length) i += 2
      else if (ch == '"') {
        inQuotes = !inQuotes
        i += 1
      } else if (ch == target && !inQuotes) {
        result = i
      } else {
        i += 1
      }
    }
    result
  }

}
