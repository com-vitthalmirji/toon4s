package io.toonformat.toon4s
package json

import scala.annotation.tailrec
import scala.collection.immutable.VectorMap
import scala.util.Try

import io.toonformat.toon4s.JsonValue
import io.toonformat.toon4s.JsonValue._

final case class JsonParseException(message: String) extends RuntimeException(message)

object SimpleJson {

  def parse(input: String): JsonValue = {
    val (value, finalIndex) = Parser.parseValue(input, 0)
    val nextIndex = Parser.skipWhitespace(input, finalIndex)
    if (nextIndex < input.length) {
      throw JsonParseException(s"Unexpected trailing content at position $nextIndex")
    }
    value
  }

  def stringify(value: JsonValue): String = value match {
  case JNull      => "null"
  case JBool(b)   => if (b) "true" else "false"
  case JNumber(n) =>
    val normalized = n.bigDecimal.stripTrailingZeros.toPlainString
    if (normalized == "-0") "0" else normalized
  case JString(s)     => quote(s)
  case JArray(values) => values.map(stringify).mkString("[", ",", "]")
  case JObj(fields)   =>
    fields.iterator
      .map {
        case (k, v) => s"${quote(k)}:${stringify(v)}"
      }
      .mkString("{", ",", "}")
  }

  def toScala(value: JsonValue): Any = value match {
  case JNull      => None
  case JBool(b)   => b
  case JNumber(n) => n
  case JString(s) => s
  case JArray(vs) => vs.map(toScala)
  case JObj(obj)  =>
    VectorMap.from(obj.iterator.map {
      case (k, v) => k -> toScala(v)
    })
  }

  private def quote(value: String): String = {
    val escaped = value.foldLeft(new StringBuilder) {
      (builder, ch) =>
        ch match {
        case '"'              => builder.append("\\\"")
        case '\\'             => builder.append("\\\\")
        case '\b'             => builder.append("\\b")
        case '\f'             => builder.append("\\f")
        case '\n'             => builder.append("\\n")
        case '\r'             => builder.append("\\r")
        case '\t'             => builder.append("\\t")
        case c if c.isControl => builder.append(f"\\u${c.toInt}%04x")
        case c                => builder.append(c)
        }
    }
    s"\"${escaped.result()}\""
  }

  private object Parser {

    @tailrec
    def skipWhitespace(input: String, index: Int): Int = {
      if (index >= input.length || !input.charAt(index).isWhitespace) index
      else skipWhitespace(input, index + 1)
    }

    def parseValue(input: String, startIndex: Int): (JsonValue, Int) = {
      val index = skipWhitespace(input, startIndex)
      if (index >= input.length) {
        throw JsonParseException(s"Unexpected end of input at position $index")
      }
      input.charAt(index) match {
      case '"' =>
        val (str, nextIndex) = parseString(input, index)
        (JString(str), nextIndex)
      case '{' => parseObject(input, index)
      case '[' => parseArray(input, index)
      case 't' =>
        val newIndex = consumeLiteral(input, index, "true")
        (JBool(true), newIndex)
      case 'f' =>
        val newIndex = consumeLiteral(input, index, "false")
        (JBool(false), newIndex)
      case 'n' =>
        val newIndex = consumeLiteral(input, index, "null")
        (JNull, newIndex)
      case ch if ch == '-' || ch.isDigit => parseNumber(input, index)
      case other                         =>
        throw JsonParseException(s"Unexpected character '$other' at position $index")
      }
    }

    private def parseString(input: String, startIndex: Int): (String, Int) = {
      val index = expect(input, startIndex, '"')

      @tailrec
      def parseChars(idx: Int, builder: StringBuilder): (String, Int) = {
        if (idx >= input.length) {
          throw JsonParseException("Unterminated string literal")
        } else if (input.charAt(idx) == '"') {
          (builder.result(), idx + 1)
        } else if (input.charAt(idx) == '\\') {
          if (idx + 1 >= input.length) {
            throw JsonParseException("Unterminated escape sequence in string literal")
          }
          val (escapedChar, charsConsumed) = input.charAt(idx + 1) match {
          case '"'  => ('"', 2)
          case '\\' => ('\\', 2)
          case '/'  => ('/', 2)
          case 'b'  => ('\b', 2)
          case 'f'  => ('\f', 2)
          case 'n'  => ('\n', 2)
          case 'r'  => ('\r', 2)
          case 't'  => ('\t', 2)
          case 'u'  =>
            if (idx + 5 >= input.length) {
              throw JsonParseException("Invalid unicode escape sequence")
            }
            val hex = input.substring(idx + 2, idx + 6)
            val codePoint = Try(Integer.parseInt(hex, 16)).getOrElse {
              throw JsonParseException(s"Invalid unicode escape: \\u$hex")
            }
            (codePoint.toChar, 6)
          case other => throw JsonParseException(s"Invalid escape sequence: \\$other")
          }
          parseChars(idx + charsConsumed, builder.append(escapedChar))
        } else {
          parseChars(idx + 1, builder.append(input.charAt(idx)))
        }
      }

      val (str, newIndex) = parseChars(index, new StringBuilder)
      (str, newIndex)
    }

    private def parseObject(input: String, startIndex: Int): (JsonValue, Int) = {
      val afterBrace = expect(input, startIndex, '{')
      val index = skipWhitespace(input, afterBrace)

      if (index < input.length && input.charAt(index) == '}') {
        (JObj(VectorMap.empty), index + 1)
      } else {
        @tailrec
        def parseFields(
            idx: Int,
            acc: Vector[(String, JsonValue)],
        ): (Vector[(String, JsonValue)], Int) = {
          val afterWs = skipWhitespace(input, idx)
          val (key, afterKey) = parseString(input, afterWs)
          val afterKeyWs = skipWhitespace(input, afterKey)
          val afterColon = expect(input, afterKeyWs, ':')
          val (value, afterValue) = parseValue(input, afterColon)
          val newAcc = acc :+ (key -> value)
          val afterValueWs = skipWhitespace(input, afterValue)

          if (afterValueWs < input.length && input.charAt(afterValueWs) == ',') {
            parseFields(afterValueWs + 1, newAcc)
          } else if (afterValueWs < input.length && input.charAt(afterValueWs) == '}') {
            (newAcc, afterValueWs + 1)
          } else {
            throw JsonParseException(s"Expected ',' or '}' at position $afterValueWs")
          }
        }

        val (fields, finalIndex) = parseFields(index, Vector.empty)
        (JObj(VectorMap.from(fields)), finalIndex)
      }
    }

    private def parseArray(input: String, startIndex: Int): (JsonValue, Int) = {
      val afterBracket = expect(input, startIndex, '[')
      val index = skipWhitespace(input, afterBracket)

      if (index < input.length && input.charAt(index) == ']') {
        (JArray(Vector.empty), index + 1)
      } else {
        @tailrec
        def parseElements(idx: Int, acc: Vector[JsonValue]): (Vector[JsonValue], Int) = {
          val (value, afterValue) = parseValue(input, idx)
          val newAcc = acc :+ value
          val afterValueWs = skipWhitespace(input, afterValue)

          if (afterValueWs < input.length && input.charAt(afterValueWs) == ',') {
            parseElements(afterValueWs + 1, newAcc)
          } else if (afterValueWs < input.length && input.charAt(afterValueWs) == ']') {
            (newAcc, afterValueWs + 1)
          } else {
            throw JsonParseException(s"Expected ',' or ']' at position $afterValueWs")
          }
        }

        val (elements, finalIndex) = parseElements(index, Vector.empty)
        (JArray(elements), finalIndex)
      }
    }

    private def parseNumber(input: String, startIndex: Int): (JsonValue, Int) = {
      val start = startIndex
      val afterSign = if (startIndex < input.length && input.charAt(startIndex) == '-') {
        startIndex + 1
      } else {
        startIndex
      }

      val afterDigits = readDigitsOrThrow(input, afterSign, "digit")

      val afterDecimal = if (afterDigits < input.length && input.charAt(afterDigits) == '.') {
        val afterPoint = afterDigits + 1
        readDigitsOrThrow(input, afterPoint, "digit after decimal point")
      } else {
        afterDigits
      }

      val afterExponent =
        if (
            afterDecimal < input.length &&
            (input.charAt(afterDecimal) == 'e' || input.charAt(afterDecimal) == 'E')
        ) {
          val afterE = afterDecimal + 1
          val afterExpSign =
            if (
                afterE < input.length &&
                (input.charAt(afterE) == '+' || input.charAt(afterE) == '-')
            ) {
              afterE + 1
            } else {
              afterE
            }
          readDigitsOrThrow(input, afterExpSign, "digit in exponent")
        } else {
          afterDecimal
        }

      val literal = input.substring(start, afterExponent)
      val number = Try(BigDecimal(literal)).fold(
        _ => throw JsonParseException(s"Invalid number literal: $literal"),
        identity,
      )
      (JNumber(number), afterExponent)
    }

    @tailrec
    private def readDigitsOrThrow(input: String, index: Int, context: String): Int = {
      if (index >= input.length || !input.charAt(index).isDigit) {
        if (index == 0 || (index > 0 && !input.charAt(index - 1).isDigit)) {
          throw JsonParseException(s"Expected $context at position $index")
        }
        index
      } else {
        readDigitsOrThrow(input, index + 1, context)
      }
    }

    private def consumeLiteral(input: String, index: Int, expected: String): Int = {
      if (!input.substring(index).startsWith(expected)) {
        throw JsonParseException(s"Expected literal '$expected' at position $index")
      }
      index + expected.length
    }

    private def expect(input: String, index: Int, char: Char): Int = {
      if (index >= input.length || input.charAt(index) != char) {
        throw JsonParseException(s"Expected '$char' at position $index")
      }
      index + 1
    }

  }

}
