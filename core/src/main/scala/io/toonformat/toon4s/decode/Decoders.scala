package io.toonformat.toon4s
package decode

import scala.annotation.tailrec
import scala.collection.immutable.VectorMap
import scala.collection.mutable.ArrayBuffer

import Parser._
import Validation._
import io.toonformat.toon4s.{Constants => C, JsonValue}
import io.toonformat.toon4s.{DecodeOptions, Delimiter}
import io.toonformat.toon4s.JsonValue._
import io.toonformat.toon4s.error.{DecodeError, ErrorLocation}

object Decoders {

  def decode(input: String, options: DecodeOptions): JsonValue = {
    val isStrict = options.strictness == Strictness.Strict
    val scan = Scanner.toParsedLines(input, options.indent, isStrict)
    decodeScan(scan, options)
  }

  /** Parse primitive token with string length validation. */
  private def parsePrimitiveWithValidation(token: String, options: DecodeOptions): JsonValue = {
    validateStringLength(token.length, options)
    val result = parsePrimitiveToken(token)
    result match {
    case JString(s) => validateStringLength(s.length, options)
    case _          => ()
    }
    result
  }

  def decodeScan(scan: ScanResult, options: DecodeOptions): JsonValue = {
    if (scan.lines.isEmpty) JObj(VectorMap.empty)
    else {
      val cursor = new LineCursor(scan.lines, scan.blanks)
      implicit val strictness: Strictness = options.strictness

      val rootArray = cursor.peek.flatMap {
        first =>
          if (isArrayHeaderAfterHyphen(first.content)) {
            parseArrayHeaderLine(first.content, Delimiter.Comma).map {
              case (header, inline) =>
                cursor.advance()
                decodeArrayFromHeader(header, inline, cursor, 0, options)
            }
          } else None
      }

      rootArray.getOrElse {
        if (scan.lines.length == 1 && cursor.peek.exists(line => !isKeyValueLine(line)))
          parsePrimitiveWithValidation(cursor.peek.get.content.trim, options)
        else decodeObject(cursor, 0, options)
      }
    }
  }

  private def isKeyValueLine(line: ParsedLine): Boolean = {
    val content = line.content
    content.headOption match {
    case Some('"') =>
      val closing = findClosingQuote(content, 0)
      closing != -1 && content.substring(closing + 1).contains(C.Colon)
    case _ => content.contains(C.Colon)
    }
  }

  final private case class KeyValueParse(key: String, value: JsonValue, followDepth: Int)

  private def decodeObject(
      cursor: LineCursor,
      baseDepth: Int,
      options: DecodeOptions,
  ): JsonValue = {
    validateDepth(baseDepth, options)
    val builder = Vector.newBuilder[(String, JsonValue)]
    var targetDepth = Option.empty[Int]
    var continue = true
    while (continue) {
      cursor.peek match {
      case None                                 => continue = false
      case Some(line) if line.depth < baseDepth => continue = false
      case Some(line)                           =>
        val td = targetDepth.orElse(Some(line.depth))
        if (td.contains(line.depth)) {
          cursor.advance()
          val KeyValueParse(key, value, _) =
            decodeKeyValue(line.content, cursor, line.depth, options)
          builder += ((key, value))
          targetDepth = td
        } else continue = false
      }
    }
    JObj(VectorMap.from(builder.result()))
  }

  private def decodeKeyValue(
      content: String,
      cursor: LineCursor,
      baseDepth: Int,
      options: DecodeOptions,
  ): KeyValueParse = {
    parseArrayHeaderLine(content, Delimiter.Comma) match {
    case Some((header, inline)) if header.key.nonEmpty =>
      val arrayValue = decodeArrayFromHeader(header, inline, cursor, baseDepth, options)
      KeyValueParse(header.key.get, arrayValue, baseDepth + 1)
    case _ =>
      val (key, restIndex) = parseKeyToken(content, 0)
      val rest = content.substring(restIndex).trim
      if (rest.isEmpty) {
        cursor.peek match {
        case Some(next) if next.depth > baseDepth =>
          val nested = decodeObject(cursor, baseDepth + 1, options)
          KeyValueParse(key, nested, baseDepth + 1)
        case _ =>
          KeyValueParse(key, JObj(VectorMap.empty), baseDepth + 1)
        }
      } else {
        KeyValueParse(key, parsePrimitiveWithValidation(rest, options), baseDepth + 1)
      }
    }
  }

  private def decodeArrayFromHeader(
      header: ArrayHeaderInfo,
      inlineValues: Option[String],
      cursor: LineCursor,
      baseDepth: Int,
      options: DecodeOptions,
  ): JsonValue = {
    inlineValues match {
    case Some(inline) => decodeInlinePrimitiveArray(header, inline, options)
    case None         =>
      if (header.fields.nonEmpty) JArray(decodeTabularArray(header, cursor, baseDepth, options))
      else JArray(decodeListArray(header, cursor, baseDepth, options))
    }
  }

  private def decodeInlinePrimitiveArray(
      header: ArrayHeaderInfo,
      inlineValues: String,
      options: DecodeOptions,
  ): JsonValue = {
    validateArrayLength(header.length, options)
    if (inlineValues.trim.isEmpty) {
      assertExpectedCount(0, header.length, "inline array items")(options.strictness)
      JArray(Vector.empty)
    } else {
      val values = parseDelimitedValues(inlineValues, header.delimiter)
      val primitives = mapRowValuesToPrimitives(values)
      assertExpectedCount(primitives.length, header.length, "inline array items")(
        options.strictness
      )
      JArray(primitives)
    }
  }

  private def decodeListArray(
      header: ArrayHeaderInfo,
      cursor: LineCursor,
      baseDepth: Int,
      options: DecodeOptions,
  ): Vector[JsonValue] = {
    validateArrayLength(header.length, options)
    val buffer = ArrayBuffer.empty[JsonValue]
    val itemDepth = baseDepth + 1
    var startLine: Option[Int] = None
    var endLine: Option[Int] = None
    var continue = true

    while (continue && !cursor.atEnd && buffer.length < header.length) {
      cursor.peek match {
      case Some(line) if line.depth >= itemDepth =>
        val isListItem =
          line.content.startsWith(C.ListItemPrefix) || line.content == C.ListItemMarker
        if (line.depth == itemDepth && isListItem) {
          if (startLine.isEmpty) startLine = Some(line.lineNumber)
          val item = decodeListItem(cursor, itemDepth, options)
          buffer += item
          cursor.current.foreach(cur => endLine = Some(cur.lineNumber))
        } else {
          continue = false
        }
      case _ =>
        continue = false
      }
    }

    val items: Vector[JsonValue] = buffer.toVector
    def runValidations(): Unit = {
      assertExpectedCount(items.length, header.length, "list array items")(options.strictness)
      for {
        start <- startLine
        end <- endLine
      } validateNoBlankLinesInRange(start, end, cursor.getBlankLines, "list array")(
        options.strictness
      )
      validateNoExtraListItems(cursor, itemDepth, header.length)(options.strictness)
    }
    runValidations()
    items
  }

  private def decodeTabularArray(
      header: ArrayHeaderInfo,
      cursor: LineCursor,
      baseDepth: Int,
      options: DecodeOptions,
  ): Vector[JsonValue] = {
    validateArrayLength(header.length, options)
    val rows = ArrayBuffer.empty[JsonValue]
    val rowDepth = baseDepth + 1
    var startLine: Option[Int] = None
    var endLine: Option[Int] = None
    var continue = true

    while (continue && !cursor.atEnd && rows.length < header.length) {
      cursor.peek match {
      case Some(line) if line.depth == rowDepth =>
        if (startLine.isEmpty) startLine = Some(line.lineNumber)
        cursor.advance()
        val values: Vector[String] = parseDelimitedValues(line.content, header.delimiter)
        assertExpectedCount(values.length, header.fields.length, "tabular row values")(
          options.strictness
        )
        val primitives: Vector[JsonValue] = mapRowValuesToPrimitives(values)
        val obj = VectorMap.from(header.fields.zip(primitives))
        rows += JObj(obj)
        cursor.current.foreach(cur => endLine = Some(cur.lineNumber))
      case Some(line) if line.depth < rowDepth =>
        continue = false
      case Some(_) =>
        continue = false
      case None =>
        continue = false
      }
    }

    val table: Vector[JsonValue] = rows.toVector
    def runValidations(): Unit = {
      assertExpectedCount(table.length, header.length, "tabular rows")(options.strictness)
      for {
        start <- startLine
        end <- endLine
      } validateNoBlankLinesInRange(start, end, cursor.getBlankLines, "tabular array")(
        options.strictness
      )
      validateNoExtraTabularRows(cursor, rowDepth, header)(options.strictness)
    }
    runValidations()
    table
  }

  private def decodeListItem(
      cursor: LineCursor,
      baseDepth: Int,
      options: DecodeOptions,
  ): JsonValue = {
    validateDepth(baseDepth, options)
    val line = cursor.next().getOrElse(throw new NoSuchElementException("Expected list item"))
    val content = line.content
    val emptyObject = JObj(VectorMap.empty)
    if (content == C.ListItemMarker) emptyObject
    else {
      val afterHyphen =
        if (content.startsWith(C.ListItemPrefix)) content.drop(C.ListItemPrefix.length)
        else
          throw DecodeError.Syntax(
            s"""Expected list item to start with "${C.ListItemPrefix}"""",
            Some(ErrorLocation(line.lineNumber, 1, line.raw)),
          )

      if (afterHyphen.trim.isEmpty) emptyObject
      else {
        val arrayValue =
          if (isArrayHeaderAfterHyphen(afterHyphen))
            parseArrayHeaderLine(afterHyphen, Delimiter.Comma).map {
              case (header, inline) =>
                decodeArrayFromHeader(header, inline, cursor, baseDepth, options)
            }
          else None

        arrayValue
          .orElse {
            Option.when(isObjectFirstFieldAfterHyphen(afterHyphen)) {
              decodeObjectFromListItem(line, cursor, baseDepth, options)
            }
          }
          .getOrElse(parsePrimitiveWithValidation(afterHyphen, options))
      }
    }
  }

  private def decodeObjectFromListItem(
      firstLine: ParsedLine,
      cursor: LineCursor,
      baseDepth: Int,
      options: DecodeOptions,
  ): JsonValue = {
    val afterHyphen = firstLine.content.drop(C.ListItemPrefix.length)
    val KeyValueParse(firstKey, firstValue, followDepth) =
      decodeKeyValue(afterHyphen, cursor, baseDepth, options)
    val builder = Vector.newBuilder[(String, JsonValue)]
    builder += ((firstKey, firstValue))
    var continue = true

    while (continue && !cursor.atEnd) {
      cursor.peek match {
      case Some(line) if line.depth < followDepth =>
        continue = false
      case Some(line)
          if line.depth == followDepth && !line.content.startsWith(C.ListItemPrefix) =>
        cursor.advance()
        val KeyValueParse(k, v, _) = decodeKeyValue(line.content, cursor, followDepth, options)
        builder += ((k, v))
      case _ =>
        continue = false
      }
    }

    JObj(VectorMap.from(builder.result()))
  }

}
