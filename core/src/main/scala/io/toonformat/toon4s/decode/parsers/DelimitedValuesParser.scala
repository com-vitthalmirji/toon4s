package io.toonformat.toon4s
package decode
package parsers

import io.toonformat.toon4s.{Delimiter, JsonValue}
import io.toonformat.toon4s.error.DecodeError

/**
 * Parser for delimiter-separated values with quote awareness.
 *
 * ==Design: Single responsibility principle + State machine==
 *
 * This object handles only delimited value parsing:
 *   - Splitting strings on delimiters (respecting quotes)
 *   - Preserving escaped characters within quotes
 *   - Converting values to primitives for tabular data
 *
 * ==State machine==
 * Tracks whether we're inside quotes to know when delimiters are significant:
 *   - Outside quotes: delimiter splits values
 *   - Inside quotes: delimiter is part of the value
 *
 * @example
 *   {{{
 * DelimitedValuesParser.parseDelimitedValues("a,b,c", Delimiter.Comma)
 * // Result: Vector("a", "b", "c")
 *
 * DelimitedValuesParser.parseDelimitedValues("\"a,b\",c", Delimiter.Comma)
 * // Result: Vector("\"a,b\"", "c") - comma inside quotes is preserved
 *   }}}
 */
object DelimitedValuesParser {

  /**
   * Parse a delimited string into a vector of value strings.
   *
   * ==Stateful parsing with immutable result==
   * Uses mutable state during parsing but returns immutable Vector.
   *
   * @param input
   *   The input string containing delimited values
   * @param delim
   *   The delimiter to split on
   * @return
   *   Vector of trimmed value strings
   *
   * @example
   *   {{{
   * parseDelimitedValues("1,2,3", Delimiter.Comma)
   * // Vector("1", "2", "3")
   *
   * parseDelimitedValues("a\tb\tc", Delimiter.Tab)
   * // Vector("a", "b", "c")
   *
   * parseDelimitedValues("\"x,y\",z", Delimiter.Comma)
   * // Vector("\"x,y\"", "z") - quoted comma preserved
   *
   * parseDelimitedValues("a,\"b\\\"c\",d", Delimiter.Comma)
   * // Vector("a", "\"b\\\"c\"", "d") - escaped quote preserved
   *   }}}
   */
  def parseDelimitedValues(input: String, delim: Delimiter): Vector[String] = {
    val out = scala.collection.mutable.ArrayBuffer.empty[String]
    val builder = new StringBuilder
    var i = 0
    var inQuotes = false

    while (i < input.length) {
      val ch = input.charAt(i)

      // Handle escape sequences inside quotes
      if (ch == '\\' && i + 1 < input.length && inQuotes) {
        builder.append(ch).append(input.charAt(i + 1))
        i += 2
      }
      // Toggle quote state
      else if (ch == '"') {
        inQuotes = !inQuotes
        builder.append(ch)
        i += 1
      }
      // Delimiter outside quotes - split here
      else if (ch == delim.char && !inQuotes) {
        out += builder.result().trim
        builder.clear()
        i += 1
      }
      // Regular character - append
      else {
        builder.append(ch)
        i += 1
      }
    }

    // Add final value if present
    if (builder.nonEmpty || out.nonEmpty) out += builder.result().trim

    out.toVector
  }

  /**
   * Map string values to primitive JsonValues for tabular data.
   *
   * ==Validation plan==
   * Only allows primitive types in tabular rows:
   *   - Strings (quoted or unquoted)
   *   - Numbers
   *   - Booleans
   *   - Null
   *
   * Arrays and objects are not allowed in tabular cells.
   *
   * @param values
   *   Vector of string values to convert
   * @return
   *   Vector of JsonValue primitives
   * @throws DecodeError.Syntax
   *   if a value is not a primitive (array/object)
   *
   * @example
   *   {{{
   * mapRowValuesToPrimitives(Vector("42", "true", "\"text\""))
   * // Vector(JNumber(42), JBool(true), JString("text"))
   *
   * mapRowValuesToPrimitives(Vector("1", "null", "3.14"))
   * // Vector(JNumber(1), JNull, JNumber(3.14))
   *   }}}
   */
  def mapRowValuesToPrimitives(values: Vector[String]): Vector[JsonValue] = {
    values.map {
      token =>
        PrimitiveParser.parsePrimitiveToken(token) match {
        case s: JsonValue.JString => s
        case n: JsonValue.JNumber => n
        case b: JsonValue.JBool   => b
        case JsonValue.JNull      => JsonValue.JNull
        case other                =>
          throw DecodeError.Syntax(
            s"Tabular rows must contain primitive values, but found: $other"
          )
        }
    }
  }

}
