package io.toonformat.toon4s
package decode

import scala.annotation.tailrec
import scala.collection.immutable.VectorMap

import io.toonformat.toon4s.{DecodeOptions, Delimiter, JsonValue, Strictness}
import io.toonformat.toon4s.JsonValue.{JArray, JNull, JObj, JString}
import io.toonformat.toon4s.decode.cursor.ImmutableLineCursor
import io.toonformat.toon4s.error.DecodeError

/**
 * Immutable decoders using ImmutableLineCursor.
 *
 * ==Design Pattern: Pure Functional Programming + State Monad==
 *
 * This object provides purely functional decoders that use [[cursor.ImmutableLineCursor]] instead
 * of mutable [[LineCursor]]. All operations return new cursor states, making the code referentially
 * transparent and easier to reason about.
 *
 * ==Strategy==
 * Instead of replacing the existing [[Decoders]] object (which works and is battle-tested), we
 * provide an alternative implementation that demonstrates the immutable pattern. Users can choose
 * which style to use.
 *
 * ==Benefits==
 *   - Referential transparency
 *   - Safe concurrent access
 *   - Backtracking capabilities
 *   - Easier testing and debugging
 *
 * ==Trade-offs==
 *   - Slightly more allocations (creating new cursor instances)
 *   - More complex type signatures (must thread cursor through operations)
 *
 * @example
 *   {{{
 * val result = ImmutableDecoders.decode(toonInput, DecodeOptions())
 * // Same API as Decoders, but uses immutable cursor internally
 *   }}}
 */
object ImmutableDecoders {

  /**
   * Decode TOON input using immutable cursor.
   *
   * ==Entry Point==
   * Main entry point that delegates to decodeScan.
   *
   * @param input
   *   TOON format input
   * @param options
   *   Decode options
   * @return
   *   Decoded JsonValue
   */
  def decode(input: String, options: DecodeOptions): JsonValue = {
    val isStrict = options.strictness == Strictness.Strict
    val scan = Scanner.toParsedLines(input, options.indent, isStrict)
    decodeScan(scan, options)
  }

  /**
   * Decode from scan result using immutable cursor.
   *
   * ==Pure Functional Entry Point==
   * Creates immutable cursor and starts decoding.
   *
   * @param scan
   *   Scan result with parsed lines
   * @param options
   *   Decode options
   * @return
   *   Decoded JsonValue
   */
  def decodeScan(scan: ScanResult, options: DecodeOptions): JsonValue = {
    if (scan.lines.isEmpty) JObj(VectorMap.empty)
    else {
      val cursor = ImmutableLineCursor.fromScanResult(scan)
      implicit val strictness: Strictness = options.strictness

      // Check for root array
      cursor.peek match {
      case Some(first) if isArrayHeaderAfterHyphen(first.content) =>
        Parser.parseArrayHeaderLine(first.content, Delimiter.Comma) match {
        case Some((header, inline)) =>
          val (result, _) = decodeArrayFromHeader(header, inline, cursor.advance, 0, options)
          result
        case None =>
          if (scan.lines.length == 1 && !isKeyValueLine(first))
            parsePrimitiveWithValidation(first.content.trim, options)
          else {
            val (obj, _) = decodeObject(cursor, 0, options)
            obj
          }
        }
      case Some(line) if scan.lines.length == 1 && !isKeyValueLine(line) =>
        parsePrimitiveWithValidation(line.content.trim, options)
      case _ =>
        val (obj, _) = decodeObject(cursor, 0, options)
        obj
      }
    }
  }

  /**
   * Parse primitive with validation.
   *
   * ==Pure Function==
   *
   * @param token
   *   Token to parse
   * @param options
   *   Decode options
   * @return
   *   JsonValue
   */
  private def parsePrimitiveWithValidation(token: String, options: DecodeOptions): JsonValue = {
    Validation.validateStringLength(token.length, options)
    val result = Parser.parsePrimitiveToken(token)
    result match {
    case JString(s) => Validation.validateStringLength(s.length, options)
    case _          => ()
    }
    result
  }

  /**
   * Check if line is key-value format.
   *
   * ==Pure Predicate Function==
   *
   * @param line
   *   Parsed line
   * @return
   *   true if line contains key-value syntax
   */
  private def isKeyValueLine(line: ParsedLine): Boolean = {
    val content = line.content
    content.headOption match {
    case Some('"') =>
      val closing = Parser.findClosingQuote(content, 0)
      closing != -1 && content.substring(closing + 1).contains(Constants.Colon)
    case _ => content.contains(Constants.Colon)
    }
  }

  /**
   * Check if content is array header after hyphen.
   *
   * ==Delegation to Parser==
   *
   * @param content
   *   Line content
   * @return
   *   true if array header format
   */
  private def isArrayHeaderAfterHyphen(content: String): Boolean =
    Parser.isArrayHeaderAfterHyphen(content)

  /**
   * Decode object from cursor.
   *
   * ==Immutable State Threading==
   * Returns tuple of (result, updated cursor).
   *
   * @param cursor
   *   Current cursor position
   * @param baseDepth
   *   Base depth level
   * @param options
   *   Decode options
   * @param strictness
   *   Strictness mode
   * @return
   *   Tuple of (JsonValue, updated cursor)
   */
  private def decodeObject(
      cursor: ImmutableLineCursor,
      baseDepth: Int,
      options: DecodeOptions,
  )(implicit strictness: Strictness): (JsonValue, ImmutableLineCursor) = {
    Validation.validateDepth(baseDepth, options)

    @tailrec
    def collectFields(
        current: ImmutableLineCursor,
        acc: VectorMap[String, JsonValue],
    ): (VectorMap[String, JsonValue], ImmutableLineCursor) = {
      current.peek match {
      case Some(line) if line.depth == baseDepth =>
        val (kvParse, nextCursor) = parseKeyValue(current, baseDepth, options)
        collectFields(nextCursor, acc + (kvParse.key -> kvParse.value))

      case _ => (acc, current)
      }
    }

    val (fields, finalCursor) = collectFields(cursor, VectorMap.empty)
    (JObj(fields), finalCursor)
  }

  /**
   * Parse key-value pair.
   *
   * ==Immutable State Handling==
   * Parses key, value, and returns updated cursor.
   *
   * @param cursor
   *   Current cursor
   * @param baseDepth
   *   Base depth
   * @param options
   *   Decode options
   * @param strictness
   *   Strictness mode
   * @return
   *   Tuple of (KeyValueParse, updated cursor)
   */
  private def parseKeyValue(
      cursor: ImmutableLineCursor,
      baseDepth: Int,
      options: DecodeOptions,
  )(implicit strictness: Strictness): (KeyValueParse, ImmutableLineCursor) = {
    val (lineOpt, nextCursor) = cursor.next

    lineOpt match {
    case Some(line) =>
      val (key, restIdx) = Parser.parseKeyToken(line.content, 0)
      val rest = line.content.substring(restIdx).trim

      if (rest.nonEmpty) {
        // Inline value
        val value = parsePrimitiveWithValidation(rest, options)
        (KeyValueParse(key, value, baseDepth), nextCursor)
      } else {
        // Value on next line(s)
        val (value, finalCursor) = decodeValue(nextCursor, baseDepth + 1, options)
        (KeyValueParse(key, value, baseDepth), finalCursor)
      }

    case None => throw DecodeError.Unexpected("Unexpected end of input")
    }
  }

  /**
   * Decode value from cursor.
   *
   * ==Pattern Matching + State Threading==
   *
   * @param cursor
   *   Current cursor
   * @param depth
   *   Current depth
   * @param options
   *   Decode options
   * @param strictness
   *   Strictness mode
   * @return
   *   Tuple of (JsonValue, updated cursor)
   */
  private def decodeValue(
      cursor: ImmutableLineCursor,
      depth: Int,
      options: DecodeOptions,
  )(implicit strictness: Strictness): (JsonValue, ImmutableLineCursor) = {
    cursor.peek match {
    case Some(line) if line.depth < depth =>
      // Empty value
      (JNull, cursor)

    case Some(line) if line.depth == depth =>
      val content = line.content

      // Check for array header
      Parser.parseArrayHeaderLine(content, Delimiter.Comma) match {
      case Some((header, inline)) =>
        decodeArrayFromHeader(header, inline, cursor.advance, depth, options)

      case None if content.startsWith(Constants.ListItemPrefix) =>
        // List array
        decodeListArray(cursor, depth, options)

      case None =>
        // Object or primitive
        if (isKeyValueLine(line)) decodeObject(cursor, depth, options)
        else {
          val value = parsePrimitiveWithValidation(content.trim, options)
          (value, cursor.advance)
        }
      }

    case None =>
      // End of input
      (JNull, cursor)

    case _ =>
      throw DecodeError.Unexpected(s"Unexpected depth at line ${cursor.position}")
    }
  }

  /**
   * Decode array from header.
   *
   * ==Array Decoding with State Threading==
   *
   * @param header
   *   Array header info
   * @param inline
   *   Optional inline values
   * @param cursor
   *   Current cursor
   * @param baseDepth
   *   Base depth
   * @param options
   *   Decode options
   * @param strictness
   *   Strictness mode
   * @return
   *   Tuple of (JsonValue, updated cursor)
   */
  private def decodeArrayFromHeader(
      header: parsers.ArrayHeaderInfo,
      inline: Option[String],
      cursor: ImmutableLineCursor,
      baseDepth: Int,
      options: DecodeOptions,
  )(implicit strictness: Strictness): (JsonValue, ImmutableLineCursor) = {
    Validation.validateArrayLength(header.length, options)

    inline match {
    case Some(values) =>
      // Inline array: arr[3]: 1,2,3
      val tokens = Parser.parseDelimitedValues(values, header.delimiter)
      Validation.assertExpectedCount(tokens.length, header.length, "array items")
      val elements = tokens.map(t => parsePrimitiveWithValidation(t.trim, options))
      (JArray(elements), cursor)

    case None if header.fields.nonEmpty =>
      // Tabular array
      decodeTabularArray(cursor, header, baseDepth, options)

    case None =>
      // List array
      decodeListArrayWithLength(cursor, header.length, baseDepth, options)
    }
  }

  /**
   * Decode list array with known length.
   *
   * @param cursor
   *   Current cursor
   * @param expectedLength
   *   Expected length
   * @param baseDepth
   *   Base depth
   * @param options
   *   Decode options
   * @param strictness
   *   Strictness mode
   * @return
   *   Tuple of (JsonValue, updated cursor)
   */
  private def decodeListArrayWithLength(
      cursor: ImmutableLineCursor,
      expectedLength: Int,
      baseDepth: Int,
      options: DecodeOptions,
  )(implicit strictness: Strictness): (JsonValue, ImmutableLineCursor) = {
    val itemDepth = baseDepth + 1

    @tailrec
    def collectItems(
        current: ImmutableLineCursor,
        acc: Vector[JsonValue],
        count: Int,
    ): (Vector[JsonValue], ImmutableLineCursor) = {
      if (count >= expectedLength) (acc, current)
      else {
        current.peek match {
        case Some(line) if line.depth == itemDepth =>
          val (value, nextCursor) = decodeValue(current, itemDepth, options)
          collectItems(nextCursor, acc :+ value, count + 1)

        case _ => (acc, current)
        }
      }
    }

    val (items, finalCursor) = collectItems(cursor, Vector.empty, 0)
    Validation.assertExpectedCount(items.length, expectedLength, "list array items")
    (JArray(items), finalCursor)
  }

  /**
   * Decode list array without length header.
   *
   * @param cursor
   *   Current cursor
   * @param depth
   *   Current depth
   * @param options
   *   Decode options
   * @param strictness
   *   Strictness mode
   * @return
   *   Tuple of (JsonValue, updated cursor)
   */
  private def decodeListArray(
      cursor: ImmutableLineCursor,
      depth: Int,
      options: DecodeOptions,
  )(implicit strictness: Strictness): (JsonValue, ImmutableLineCursor) = {
    @tailrec
    def collectItems(
        current: ImmutableLineCursor,
        acc: Vector[JsonValue],
    ): (Vector[JsonValue], ImmutableLineCursor) = {
      current.peek match {
      case Some(line)
          if line.depth == depth && line.content.startsWith(Constants.ListItemPrefix) =>
        val (value, nextCursor) = decodeValue(current, depth, options)
        collectItems(nextCursor, acc :+ value)

      case _ => (acc, current)
      }
    }

    val (items, finalCursor) = collectItems(cursor, Vector.empty)
    (JArray(items), finalCursor)
  }

  /**
   * Decode tabular array.
   *
   * @param cursor
   *   Current cursor
   * @param header
   *   Array header with fields
   * @param baseDepth
   *   Base depth
   * @param options
   *   Decode options
   * @param strictness
   *   Strictness mode
   * @return
   *   Tuple of (JsonValue, updated cursor)
   */
  private def decodeTabularArray(
      cursor: ImmutableLineCursor,
      header: parsers.ArrayHeaderInfo,
      baseDepth: Int,
      options: DecodeOptions,
  )(implicit strictness: Strictness): (JsonValue, ImmutableLineCursor) = {
    val rowDepth = baseDepth + 1

    @tailrec
    def collectRows(
        current: ImmutableLineCursor,
        acc: Vector[JsonValue],
        count: Int,
    ): (Vector[JsonValue], ImmutableLineCursor) = {
      if (count >= header.length) (acc, current)
      else {
        current.peek match {
        case Some(line) if line.depth == rowDepth =>
          val values = Parser.parseDelimitedValues(line.content, header.delimiter)
          val jsonVals = Parser.mapRowValuesToPrimitives(values)
          val obj = JObj(VectorMap.from(header.fields.zip(jsonVals)))
          collectRows(current.advance, acc :+ obj, count + 1)

        case _ => (acc, current)
        }
      }
    }

    val (rows, finalCursor) = collectRows(cursor, Vector.empty, 0)
    Validation.assertExpectedCount(rows.length, header.length, "tabular rows")
    (JArray(rows), finalCursor)
  }

  /**
   * Key-value parse result.
   *
   * ==Internal Data Structure==
   *
   * @param key
   *   Parsed key
   * @param value
   *   Parsed value
   * @param followDepth
   *   Depth to continue parsing at
   */
  final private case class KeyValueParse(key: String, value: JsonValue, followDepth: Int)

}
