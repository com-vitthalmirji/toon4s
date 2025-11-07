package io.toonformat.toon4s
package decode

import io.toonformat.toon4s.error.DecodeError
import io.toonformat.toon4s.{Constants => C}
import io.toonformat.toon4s.{Delimiter, JsonValue}
import scala.util.Try

final case class ArrayHeaderInfo(
    key: Option[String],
    length: Int,
    delimiter: Delimiter,
    fields: List[String],
    hasLengthMarker: Boolean
)

object Parser {
  def parseArrayHeaderLine(
      content: String,
      defaultDelim: Delimiter
  ): Option[(ArrayHeaderInfo, Option[String])] = {
    val trimmed = content.dropWhile(_.isWhitespace)
    if (trimmed.isEmpty) None
    else {
      var cursor                 = content.length - trimmed.length
      var keyOpt: Option[String] = None

      def skipWhitespace(pos: Int): Int = {
        var cur = pos
        while (cur < content.length && content.charAt(cur).isWhitespace) cur += 1
        cur
      }

      val keyParsed = trimmed.headOption match {
        case Some('"') =>
          val relativeClosing = findClosingQuote(trimmed, 0)
          if (relativeClosing < 0) false
          else {
            val rawKey = trimmed.substring(0, relativeClosing + 1)
            keyOpt = Some(parseStringLiteral(rawKey))
            cursor += relativeClosing + 1
            true
          }
        case Some('[') => true
        case _         =>
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
        val bracketEnd   = content.indexOf(']', bracketStart)
        if (
          cursor >= content.length ||
          content.charAt(cursor) != '[' ||
          bracketEnd < 0
        ) None
        else {
          val bracketSegment                 = content.substring(bracketStart + 1, bracketEnd)
          val (length, delimiter, hasMarker) = parseBracketSegment(bracketSegment, defaultDelim)
          cursor = skipWhitespace(bracketEnd + 1)

          def parseFieldsSection(start: Int): Option[(List[String], Int)] = {
            if (start < content.length && content.charAt(start) == '{') {
              val braceEnd = content.indexOf('}', start)
              if (braceEnd < 0) None
              else {
                val fieldsSegment = content.substring(start + 1, braceEnd)
                val fields        =
                  if (fieldsSegment.nonEmpty)
                    parseDelimitedValues(fieldsSegment, delimiter)
                      .map(token => parseStringLiteral(token.trim))
                      .toList
                  else Nil
                Some(fields -> skipWhitespace(braceEnd + 1))
              }
            } else Some(Nil -> skipWhitespace(start))
          }

          parseFieldsSection(cursor).flatMap {
            case (fields, nextCursor) =>
              val colonCursor = skipWhitespace(nextCursor)
              if (colonCursor >= content.length || content.charAt(colonCursor) != ':') None
              else {
                val inline = content.substring(colonCursor + 1).trim match {
                  case ""    => None
                  case other => Some(other)
                }
                Some(ArrayHeaderInfo(keyOpt, length, delimiter, fields, hasMarker) -> inline)
              }
          }
        }
      }
    }
  }

  def parseBracketSegment(seg: String, defaultDelim: Delimiter): (Int, Delimiter, Boolean) = {
    var hasMarker = false
    var content   = seg
    if (content.startsWith("#")) {
      hasMarker = true
      content = content.drop(1)
    }
    var delimiter = defaultDelim
    if (content.endsWith("\t")) {
      delimiter = Delimiter.Tab
      content = content.dropRight(1)
    } else if (content.endsWith("|")) {
      delimiter = Delimiter.Pipe
      content = content.dropRight(1)
    }
    val len       = content.toIntOption.getOrElse {
      throw DecodeError.InvalidHeader(s"Invalid array length: $seg")
    }
    (len, delimiter, hasMarker)
  }

  def parseDelimitedValues(input: String, delim: Delimiter): List[String] = {
    val out      = List.newBuilder[String]
    val builder  = new StringBuilder
    var i        = 0
    var inQuotes = false
    while (i < input.length) {
      val ch = input.charAt(i)
      if (ch == '\\' && i + 1 < input.length && inQuotes) {
        builder.append(ch).append(input.charAt(i + 1))
        i += 2
      } else if (ch == '"') {
        inQuotes = !inQuotes
        builder.append(ch)
        i += 1
      } else if (ch == delim.char && !inQuotes) {
        out += builder.result().trim
        builder.clear()
        i += 1
      } else {
        builder.append(ch)
        i += 1
      }
    }
    if (builder.nonEmpty || out.result().nonEmpty) out += builder.result().trim
    out.result()
  }

  def mapRowValuesToPrimitives(values: List[String]): Vector[JsonValue] = {
    values.map {
      token =>
        parsePrimitiveToken(token) match {
          case s: JsonValue.JString => s
          case n: JsonValue.JNumber => n
          case b: JsonValue.JBool   => b
          case JsonValue.JNull      => JsonValue.JNull
          case other                =>
            throw DecodeError.Syntax(
              s"Tabular rows must contain primitive values, but found: $other"
            )
        }
    }.toVector
  }

  def parsePrimitiveToken(token: String): JsonValue = {
    val trimmed = token.trim
    if (trimmed.isEmpty) JsonValue.JString("")
    else if (trimmed.headOption.contains('"')) JsonValue.JString(parseStringLiteral(trimmed))
    else if (isBooleanOrNullLiteral(trimmed))
      trimmed match {
        case C.TrueLiteral  => JsonValue.JBool(true)
        case C.FalseLiteral => JsonValue.JBool(false)
        case _              => JsonValue.JNull
      }
    else if (isNumericLiteral(trimmed)) {
      val bd   = BigDecimal(trimmed)
      val norm = if (bd == BigDecimal(0) && trimmed.startsWith("-")) BigDecimal(0) else bd
      JsonValue.JNumber(norm)
    } else JsonValue.JString(trimmed)
  }

  private def isBooleanOrNullLiteral(value: String): Boolean =
    value == C.TrueLiteral || value == C.FalseLiteral || value == C.NullLiteral

  private def isNumericLiteral(token: String): Boolean = {
    if (token.isEmpty) false
    else {
      val unsigned =
        if (token.head == '-' || token.head == '+') token.tail else token
      val hasLeadingZero = unsigned.length > 1 && unsigned.head == '0' && unsigned(1) != '.'
      unsigned.nonEmpty && !hasLeadingZero && Try(BigDecimal(token)).isSuccess
    }
  }

  def parseStringLiteral(token: String): String = {
    val trimmed = token.trim
    trimmed.headOption.filter(_ == '"') match {
      case Some(_) =>
        val end = findClosingQuote(trimmed, 0)
        if (end < 0 || end != trimmed.length - 1)
          throw DecodeError.Syntax("Unterminated or trailing content after string literal")
        unescapeString(trimmed.slice(1, end))
      case None    => trimmed
    }
  }

  def findClosingQuote(content: String, start: Int): Int = {
    var i       = start + 1
    var closed  = -1
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

  def unescapeString(s: String): String = {
    val builder = new StringBuilder
    var i       = 0
    while (i < s.length) {
      s.charAt(i) match {
        case '\\' if i + 1 < s.length =>
          s.charAt(i + 1) match {
            case '"'                     => builder.append('"')
            case '\\'                    => builder.append('\\')
            case 'n'                     => builder.append('\n')
            case 'r'                     => builder.append('\r')
            case 't'                     => builder.append('\t')
            case 'u' if i + 5 < s.length =>
              val hex   = s.substring(i + 2, i + 6)
              val value = Try(Integer.parseInt(hex, 16)).getOrElse {
                throw DecodeError.Syntax(s"Invalid unicode escape: \\u$hex")
              }
              builder.append(value.toChar)
              i += 4
            case 'u'                    =>
              throw DecodeError.Syntax("Invalid unicode escape sequence")
            case other                   =>
              throw DecodeError.Syntax(s"Invalid escape sequence: \\$other")
          }
          i += 2
        case '\\'                      =>
          throw DecodeError.Syntax("Unterminated escape sequence in string literal")
        case c                        =>
          builder.append(c)
          i += 1
      }
    }
    builder.result()
  }

  def parseUnquotedKey(content: String, start: Int): (String, Int) = {
    var end = start
    while (end < content.length && content.charAt(end) != C.Colon) end += 1
    if (end >= content.length || content.charAt(end) != C.Colon)
      throw DecodeError.Syntax("Missing colon after key")
    val key = content.substring(start, end).trim
    (key, end + 1)
  }

  def parseQuotedKey(content: String, start: Int): (String, Int) = {
    val closing = findClosingQuote(content, start)
    if (closing < 0) throw DecodeError.Syntax("Unterminated quoted key")
    if (closing + 1 >= content.length || content.charAt(closing + 1) != C.Colon)
      throw DecodeError.Syntax("Missing colon after key")
    val key     = unescapeString(content.substring(start + 1, closing))
    (key, closing + 2)
  }

  def parseKeyToken(content: String, start: Int): (String, Int) =
    if (content.charAt(start) == '"') parseQuotedKey(content, start)
    else parseUnquotedKey(content, start)

  def findUnquotedChar(content: String, target: Char): Int = {
    var inQuotes = false
    var i        = 0
    var result   = -1
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

  def isArrayHeaderAfterHyphen(content: String): Boolean =
    content.trim.startsWith("[") && findUnquotedChar(content, C.Colon) != -1

  def isObjectFirstFieldAfterHyphen(content: String): Boolean =
    findUnquotedChar(content, C.Colon) != -1
}
