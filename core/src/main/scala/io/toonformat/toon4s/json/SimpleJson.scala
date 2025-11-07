package io.toonformat.toon4s
package json

import io.toonformat.toon4s.JsonValue
import io.toonformat.toon4s.JsonValue._
import scala.collection.immutable.VectorMap
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

final case class JsonParseException(message: String) extends RuntimeException(message)

object SimpleJson {

  def parse(input: String): JsonValue = {
    val parser = new Parser(input)
    val value  = parser.parseValue()
    parser.skipWhitespace()
    if (!parser.atEnd) {
      throw JsonParseException(s"Unexpected trailing content at position ${parser.position}")
    }
    value
  }

  def stringify(value: JsonValue): String = value match {
    case JNull          => "null"
    case JBool(b)       => if (b) "true" else "false"
    case JNumber(n)     =>
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
    case JNull      => null
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
    val builder = new StringBuilder
    builder.append('"')
    value.foreach {
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
    builder.append('"')
    builder.result()
  }

  private final class Parser(input: String) {
    private var index = 0

    def position: Int = index

    def atEnd: Boolean = index >= input.length

    def skipWhitespace(): Unit = {
      while (!atEnd && input.charAt(index).isWhitespace) {
        index += 1
      }
    }

    def parseValue(): JsonValue = {
      skipWhitespace()
      if (atEnd) throw JsonParseException(s"Unexpected end of input at position $index")
      input.charAt(index) match {
        case '"'                           => JString(parseString())
        case '{'                           => parseObject()
        case '['                           => parseArray()
        case 't'                           => consumeLiteral("true"); JBool(true)
        case 'f'                           => consumeLiteral("false"); JBool(false)
        case 'n'                           => consumeLiteral("null"); JNull
        case ch if ch == '-' || ch.isDigit => parseNumber()
        case other                         =>
          throw JsonParseException(s"Unexpected character '$other' at position $index")
      }
    }

    private def parseString(): String = {
      expect('"')
      val builder = new StringBuilder
      while (!atEnd && input.charAt(index) != '"') {
        val ch = input.charAt(index)
        if (ch == '\\') {
          if (index + 1 >= input.length) {
            throw JsonParseException("Unterminated escape sequence in string literal")
          }
          val escape = input.charAt(index + 1)
          escape match {
            case '"'   => builder.append('"')
            case '\\'  => builder.append('\\')
            case '/'   => builder.append('/')
            case 'b'   => builder.append('\b')
            case 'f'   => builder.append('\f')
            case 'n'   => builder.append('\n')
            case 'r'   => builder.append('\r')
            case 't'   => builder.append('\t')
            case 'u'   =>
              if (index + 5 >= input.length) {
                throw JsonParseException("Invalid unicode escape sequence")
              }
              val hex       = input.substring(index + 2, index + 6)
              val codePoint = Try(Integer.parseInt(hex, 16)).getOrElse {
                throw JsonParseException(s"Invalid unicode escape: \\u$hex")
              }
              builder.append(codePoint.toChar)
              index += 4
            case other => throw JsonParseException(s"Invalid escape sequence: \\$other")
          }
          index += 2
        } else {
          builder.append(ch)
          index += 1
        }
      }
      if (atEnd) throw JsonParseException("Unterminated string literal")
      index += 1
      builder.result()
    }

    private def parseObject(): JsonValue = {
      expect('{')
      skipWhitespace()
      if (peek('}')) {
        index += 1
        JObj(VectorMap.empty)
      } else {
        val b        = Vector.newBuilder[(String, JsonValue)]
        var continue = true
        while (continue) {
          skipWhitespace()
          val key   = parseString()
          skipWhitespace()
          expect(':')
          val value = parseValue()
          b += ((key, value))
          skipWhitespace()
          if (peek(',')) {
            index += 1
          } else if (peek('}')) {
            index += 1
            continue = false
          } else {
            throw JsonParseException(s"Expected ',' or '}' at position $index")
          }
        }
        JObj(VectorMap.from(b.result()))
      }
    }

    private def parseArray(): JsonValue = {
      expect('[')
      skipWhitespace()
      val values = ArrayBuffer.empty[JsonValue]
      if (peek(']')) {
        index += 1
      } else {
        var continue = true
        while (continue) {
          val value = parseValue()
          values += value
          skipWhitespace()
          if (peek(',')) {
            index += 1
          } else if (peek(']')) {
            index += 1
            continue = false
          } else {
            throw JsonParseException(s"Expected ',' or ']' at position $index")
          }
        }
      }
      JArray(values.toVector)
    }

    private def parseNumber(): JsonValue = {
      val start   = index
      if (peek('-')) index += 1
      readDigitsOrThrow("digit")
      if (peek('.')) {
        index += 1
        readDigitsOrThrow("digit after decimal point")
      }
      if (peek('e') || peek('E')) {
        index += 1
        if (peek('+') || peek('-')) index += 1
        readDigitsOrThrow("digit in exponent")
      }
      val literal = input.substring(start, index)
      Try(BigDecimal(literal)).fold(
        _ => throw JsonParseException(s"Invalid number literal: $literal"),
        number => JNumber(number)
      )
    }

    private def readDigitsOrThrow(context: String): Unit = {
      if (atEnd || !input.charAt(index).isDigit) {
        throw JsonParseException(s"Expected $context at position $index")
      }
      while (!atEnd && input.charAt(index).isDigit) index += 1
    }

    private def consumeLiteral(expected: String): Unit = {
      if (!input.substring(index).startsWith(expected)) {
        throw JsonParseException(s"Expected literal '$expected' at position $index")
      }
      index += expected.length
    }

    private def expect(char: Char): Unit = {
      if (atEnd || input.charAt(index) != char) {
        throw JsonParseException(s"Expected '$char' at position $index")
      }
      index += 1
    }

    private def peek(char: Char): Boolean = !atEnd && input.charAt(index) == char
  }
}
