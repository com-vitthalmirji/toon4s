package io.toonformat.toon4s
package decode
package parsers

import io.toonformat.toon4s.{Constants => C, Delimiter}
import io.toonformat.toon4s.error.DecodeError

/**
 * Parser for TOON array headers.
 *
 * ==Design: Single responsibility principle + Parser combinators==
 *
 * This object handles only array header parsing:
 *   - Extracting key names
 *   - Parsing bracket segments `[N]`, `[N\t]`, etc.
 *   - Parsing field lists `{field1,field2}`
 *   - Finding inline values after colon
 *
 * ==Header Syntax==
 * {{{
 * key[length]delim{fields}: inline_values
 * ^   ^       ^     ^        ^
 * |   |       |     |        optional inline values
 * |   |       |     optional field names
 * |   |       optional delimiter marker
 * |   required length
 * optional key name
 * }}}
 *
 * @example
 *   {{{
 * ArrayHeaderParser.parseArrayHeaderLine("arr[3]: 1,2,3", Delimiter.Comma)
 * // Some((ArrayHeaderInfo(Some("arr"), 3, Comma, Nil), Some("1,2,3")))
 *
 * ArrayHeaderParser.parseArrayHeaderLine("users[2]{id,name}:", Delimiter.Comma)
 * // Some((ArrayHeaderInfo(Some("users"), 2, Comma, List("id","name")), None))
 *   }}}
 */
object ArrayHeaderParser {

  /**
   * Parse an array header line with all its components.
   *
   * ==Functional parsing with Option chaining==
   * Uses Option monad for parser combinators without external libraries.
   *
   * @param content
   *   The line content to parse
   * @param defaultDelim
   *   Default delimiter if not specified in header
   * @return
   *   Option of (header info, optional inline values after colon)
   *
   * @example
   *   {{{
   * parseArrayHeaderLine("data[5]: a,b,c,d,e", Delimiter.Comma)
   * // Some((ArrayHeaderInfo(...), Some("a,b,c,d,e")))
   *
   * parseArrayHeaderLine("[3]\t:", Delimiter.Comma)
   * // Some((ArrayHeaderInfo(..., Delimiter.Tab, ...), None))
   *
   * parseArrayHeaderLine("not an array", Delimiter.Comma)
   * // None
   *   }}}
   */
  def parseArrayHeaderLine(
      content: String,
      defaultDelim: Delimiter,
  ): Option[(ArrayHeaderInfo, Option[String])] = {
    val trimmed = content.dropWhile(_.isWhitespace)
    if (trimmed.isEmpty) None
    else {
      var cursor = content.length - trimmed.length
      var keyOpt: Option[String] = None

      /**
       * Skip whitespace characters starting from position.
       *
       * ==Pure Helper Function==
       * Stateless, returns new position.
       */
      def skipWhitespace(pos: Int): Int = {
        var cur = pos
        while (cur < content.length && content.charAt(cur).isWhitespace) cur += 1
        cur
      }

      // Phase 1: Parse optional key before bracket
      val keyParsed = trimmed.headOption match {
      case Some('"') =>
        // Quoted key
        val relativeClosing = StringLiteralParser.findClosingQuote(trimmed, 0)
        if (relativeClosing < 0) false
        else {
          val rawKey = trimmed.substring(0, relativeClosing + 1)
          keyOpt = Some(StringLiteralParser.parseStringLiteral(rawKey))
          cursor += relativeClosing + 1
          true
        }
      case Some('[') =>
        // No key, starts with bracket
        true
      case _ =>
        // Unquoted key
        val bracketPos = trimmed.indexOf('[')
        if (bracketPos < 0) false
        else {
          val rawKey = trimmed.substring(0, bracketPos).trim
          if (rawKey.nonEmpty) keyOpt = Some(rawKey)
          cursor += bracketPos
          true
        }
      }

      if (!keyParsed) None
      else {
        cursor = skipWhitespace(cursor)
        val bracketStart = cursor
        val bracketEnd = content.indexOf(']', bracketStart)

        // Phase 2: Parse bracket segment [length] or [#length] or [length\t]
        if (
            cursor >= content.length ||
            content.charAt(cursor) != '[' ||
            bracketEnd < 0
        ) None
        else {
          val bracketSegment = content.substring(bracketStart + 1, bracketEnd)
          val (length, delimiter) = parseBracketSegment(bracketSegment, defaultDelim)
          cursor = skipWhitespace(bracketEnd + 1)

          // Phase 3: Parse optional field list {field1,field2,...}
          def parseFieldsSection(start: Int): Option[(List[String], Int)] = {
            if (start < content.length && content.charAt(start) == '{') {
              val braceEnd = content.indexOf('}', start)
              if (braceEnd < 0) None
              else {
                val fieldsSegment = content.substring(start + 1, braceEnd)
                val fields =
                  if (fieldsSegment.nonEmpty)
                    DelimitedValuesParser
                      .parseDelimitedValues(fieldsSegment, delimiter)
                      .map(token => StringLiteralParser.parseStringLiteral(token.trim))
                      .toList
                  else Nil
                Some(fields -> skipWhitespace(braceEnd + 1))
              }
            } else Some(Nil -> skipWhitespace(start))
          }

          // Phase 4: Parse colon and optional inline values
          parseFieldsSection(cursor).flatMap {
            case (fields, nextCursor) =>
              val colonCursor = skipWhitespace(nextCursor)
              if (colonCursor >= content.length || content.charAt(colonCursor) != ':') None
              else {
                val inline = content.substring(colonCursor + 1).trim match {
                case ""    => None
                case other => Some(other)
                }
                Some(ArrayHeaderInfo(keyOpt, length, delimiter, fields) -> inline)
              }
          }
        }
      }
    }
  }

  /**
   * Parse the bracket segment to extract length, delimiter, and marker flag.
   *
   * ==Pattern matching for configuration parsing==
   *
   * Bracket segment syntax:
   *   - `5` → length 5, default delimiter
   *   - `5\t` → length 5, tab delimiter
   *   - `5|` → length 5, pipe delimiter
   *
   * @param seg
   *   The string inside brackets
   * @param defaultDelim
   *   Default delimiter to use
   * @return
   *   Tuple of (length, delimiter, has marker flag)
   * @throws io.toonformat.toon4s.error.DecodeError.InvalidHeader
   *   if length is not a valid integer
   *
   * @example
   *   {{{
   * parseBracketSegment("5", Delimiter.Comma)
   * // (5, Delimiter.Comma)
   *
   * parseBracketSegment("3\t", Delimiter.Comma)
   * // (3, Delimiter.Tab)
   *
   * parseBracketSegment("7|", Delimiter.Comma)
   * // (7, Delimiter.Pipe)
   *   }}}
   */
  def parseBracketSegment(seg: String, defaultDelim: Delimiter): (Int, Delimiter) = {
    var content = seg

    if (content.startsWith("#")) {
      throw DecodeError.InvalidHeader("Length marker syntax [#N] is not supported in TOON v2.x")
    }

    // Check for delimiter suffix
    var delimiter = defaultDelim
    if (content.endsWith("\t")) {
      delimiter = Delimiter.Tab
      content = content.dropRight(1)
    } else if (content.endsWith("|")) {
      delimiter = Delimiter.Pipe
      content = content.dropRight(1)
    }

    // Parse length
    val len = content.toIntOption.getOrElse {
      throw DecodeError.InvalidHeader(s"Invalid array length: $seg")
    }

    (len, delimiter)
  }

  /**
   * Check if content after a list hyphen is an array header.
   *
   * ==Predicate function for type detection==
   *
   * Used to distinguish between:
   *   - `- [3]: 1,2,3` (array item)
   *   - `- key: value` (object field)
   *
   * @param content
   *   Content after the `- ` prefix
   * @return
   *   true if content starts with `[` and contains an unquoted `:`
   *
   * @example
   *   {{{
   * isArrayHeaderAfterHyphen("[3]: 1,2,3")  // true
   * isArrayHeaderAfterHyphen("key: value")  // false
   * isArrayHeaderAfterHyphen("[no colon")   // false
   *   }}}
   */
  def isArrayHeaderAfterHyphen(content: String): Boolean =
    content.trim.startsWith("[") && StringLiteralParser.findUnquotedChar(content, C.Colon) != -1

  /**
   * Check if content after a list hyphen is an object field.
   *
   * ==Predicate function for type detection==
   *
   * @param content
   *   Content after the `- ` prefix
   * @return
   *   true if content contains an unquoted `:`
   *
   * @example
   *   {{{
   * isObjectFirstFieldAfterHyphen("name: Alice")  // true
   * isObjectFirstFieldAfterHyphen("just text")    // false
   *   }}}
   */
  def isObjectFirstFieldAfterHyphen(content: String): Boolean =
    StringLiteralParser.findUnquotedChar(content, C.Colon) != -1

}
