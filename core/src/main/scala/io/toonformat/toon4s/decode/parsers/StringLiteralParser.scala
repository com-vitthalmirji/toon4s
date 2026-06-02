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
   * @throws io.toonformat.toon4s.error.DecodeError.Syntax
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
    if (trimmed.isEmpty || trimmed.charAt(0) != '"') return trimmed

    // Single pass: find closing quote AND unescape
    val builder = new StringBuilder(trimmed.length - 2)
    var i = 1 // Skip opening quote
    var closed = false

    while (i < trimmed.length && !closed) {
      trimmed.charAt(i) match {
      case '\\' if i + 1 < trimmed.length =>
        trimmed.charAt(i + 1) match {
        case '"'  => builder.append('"'); i += 2
        case '\\' => builder.append('\\'); i += 2
        case 'n'  => builder.append('\n'); i += 2
        case 'r'  => builder.append('\r'); i += 2
        case 't'  => builder.append('\t'); i += 2
        case 'u'  =>
          if (i + 5 >= trimmed.length)
            throw DecodeError.Syntax(
              s"Invalid \\u escape: fewer than 4 hex digits before end of string"
            )
          val hex = trimmed.substring(i + 2, i + 6)
          if (
              !hex.forall(c =>
                (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
              )
          )
            throw DecodeError.Syntax(s"Invalid \\u escape: expected 4 hex digits, got '$hex'")
          val cp = Integer.parseInt(hex, 16)
          if (cp >= 0xD800 && cp <= 0xDFFF)
            throw DecodeError.Syntax(s"Invalid \\u$hex: lone surrogate is not permitted")
          builder.append(cp.toChar); i += 6
        case other => throw DecodeError.Syntax(s"Invalid escape sequence: \\$other")
        }
      case '\\' =>
        throw DecodeError.Syntax("Unterminated escape sequence in string literal")
      case '"' =>
        closed = true
        i += 1
      case c =>
        builder.append(c)
        i += 1
      }
    }

    if (!closed || i != trimmed.length)
      throw DecodeError.Syntax("Unterminated or trailing content after string literal")

    builder.result()
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
   * @throws io.toonformat.toon4s.error.DecodeError.Syntax
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
        case '"'  => builder.append('"'); i += 2
        case '\\' => builder.append('\\'); i += 2
        case 'n'  => builder.append('\n'); i += 2
        case 'r'  => builder.append('\r'); i += 2
        case 't'  => builder.append('\t'); i += 2
        case 'u'  =>
          if (i + 5 >= s.length)
            throw DecodeError.Syntax(
              "Invalid \\u escape: fewer than 4 hex digits before end of string"
            )
          val hex = s.substring(i + 2, i + 6)
          if (
              !hex.forall(c =>
                (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
              )
          )
            throw DecodeError.Syntax(s"Invalid \\u escape: expected 4 hex digits, got '$hex'")
          val cp = Integer.parseInt(hex, 16)
          if (cp >= 0xD800 && cp <= 0xDFFF)
            throw DecodeError.Syntax(s"Invalid \\u$hex: lone surrogate is not permitted")
          builder.append(cp.toChar); i += 6
        case other =>
          throw DecodeError.Syntax(s"Invalid escape sequence: \\$other")
        }
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
