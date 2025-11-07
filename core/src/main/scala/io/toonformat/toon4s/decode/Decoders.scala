package io.toonformat.toon4s
package decode

import io.toonformat.toon4s.{DecodeOptions, Delimiter}
import io.toonformat.toon4s.{JsonValue, Constants => C}
import io.toonformat.toon4s.JsonValue._
import Parser._
import Validation._
import io.toonformat.toon4s.error.DecodeError

import scala.annotation.tailrec
import scala.collection.immutable.VectorMap
import scala.collection.mutable.ArrayBuffer

object Decoders {

  def decode(input: String, options: DecodeOptions): JsonValue = {
    val scan = Scanner.toParsedLines(input, options.indent, options.strict)
    if (scan.lines.isEmpty) JObj(VectorMap.empty)
    else {
      val cursor = new LineCursor(scan.lines, scan.blanks)

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
        if (
          scan.lines.length == 1 && cursor.peek.exists(
            line => !isKeyValueLine(line)
          )
        )
          parsePrimitiveToken(cursor.peek.get.content.trim)
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
      case _         => content.contains(C.Colon)
    }
  }

  private final case class KeyValueParse(key: String, value: JsonValue, followDepth: Int)

  private def decodeObject(
      cursor: LineCursor,
      baseDepth: Int,
      options: DecodeOptions
  ): JsonValue = {
    @tailrec
    def loop(
        obj: VectorMap[String, JsonValue],
        computedDepth: Option[Int]
    ): VectorMap[String, JsonValue] =
      cursor.peek match {
        case None                                 => obj
        case Some(line) if line.depth < baseDepth => obj
        case Some(line)                           =>
          val targetDepth = computedDepth.orElse(Some(line.depth))
          if (targetDepth.contains(line.depth)) {
            cursor.advance()
            val KeyValueParse(key, value, _) =
              decodeKeyValue(line.content, cursor, line.depth, options)
            loop(obj.updated(key, value), targetDepth)
          } else obj
      }

    JObj(loop(VectorMap.empty, None))
  }

  private def decodeKeyValue(
      content: String,
      cursor: LineCursor,
      baseDepth: Int,
      options: DecodeOptions
  ): KeyValueParse = {
    parseArrayHeaderLine(content, Delimiter.Comma) match {
      case Some((header, inline)) if header.key.nonEmpty =>
        val arrayValue = decodeArrayFromHeader(header, inline, cursor, baseDepth, options)
        KeyValueParse(header.key.get, arrayValue, baseDepth + 1)
      case _                                             =>
        val (key, restIndex) = parseKeyToken(content, 0)
        val rest             = content.substring(restIndex).trim
        if (rest.isEmpty) {
          cursor.peek match {
            case Some(next) if next.depth > baseDepth =>
              val nested = decodeObject(cursor, baseDepth + 1, options)
              KeyValueParse(key, nested, baseDepth + 1)
            case _                                    =>
              KeyValueParse(key, JObj(VectorMap.empty), baseDepth + 1)
          }
        } else {
          KeyValueParse(key, parsePrimitiveToken(rest), baseDepth + 1)
        }
    }
  }

  private def decodeArrayFromHeader(
      header: ArrayHeaderInfo,
      inlineValues: Option[String],
      cursor: LineCursor,
      baseDepth: Int,
      options: DecodeOptions
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
      options: DecodeOptions
  ): JsonValue = {
    if (inlineValues.trim.isEmpty) {
      assertExpectedCount(0, header.length, "inline array items", options)
      JArray(Vector.empty)
    } else {
      val values     = parseDelimitedValues(inlineValues, header.delimiter)
      val primitives = mapRowValuesToPrimitives(values)
      assertExpectedCount(primitives.length, header.length, "inline array items", options)
      JArray(primitives)
    }
  }

  private def decodeListArray(
      header: ArrayHeaderInfo,
      cursor: LineCursor,
      baseDepth: Int,
      options: DecodeOptions
  ): Vector[JsonValue] = {
    val buffer                 = ArrayBuffer.empty[JsonValue]
    val itemDepth              = baseDepth + 1
    var startLine: Option[Int] = None
    var endLine: Option[Int]   = None
    var continue               = true

    while (continue && !cursor.atEnd && buffer.length < header.length) {
      cursor.peek match {
        case Some(line) if line.depth >= itemDepth =>
          val isListItem =
            line.content.startsWith(C.ListItemPrefix) || line.content == C.ListItemMarker
          if (line.depth == itemDepth && isListItem) {
            if (startLine.isEmpty) startLine = Some(line.lineNumber)
            val item = decodeListItem(cursor, itemDepth, options)
            buffer += item
            cursor.current.foreach(
              cur => endLine = Some(cur.lineNumber)
            )
          } else {
            continue = false
          }
        case _                                     =>
          continue = false
      }
    }

    val items = buffer.toVector
    assertExpectedCount(items.length, header.length, "list array items", options)
    if (options.strict) {
      for {
        start <- startLine
        end   <- endLine
      } validateNoBlankLinesInRange(start, end, cursor.getBlankLines, options.strict, "list array")
      validateNoExtraListItems(cursor, itemDepth, header.length)
    }
    items
  }

  private def decodeTabularArray(
      header: ArrayHeaderInfo,
      cursor: LineCursor,
      baseDepth: Int,
      options: DecodeOptions
  ): Vector[JsonValue] = {
    val rows                   = ArrayBuffer.empty[JsonValue]
    val rowDepth               = baseDepth + 1
    var startLine: Option[Int] = None
    var endLine: Option[Int]   = None
    var continue               = true

    while (continue && !cursor.atEnd && rows.length < header.length) {
      cursor.peek match {
        case Some(line) if line.depth == rowDepth =>
          if (startLine.isEmpty) startLine = Some(line.lineNumber)
          cursor.advance()
          val values     = parseDelimitedValues(line.content, header.delimiter)
          assertExpectedCount(values.length, header.fields.length, "tabular row values", options)
          val primitives = mapRowValuesToPrimitives(values)
          val obj        = VectorMap.from(header.fields.zip(primitives))
          rows += JObj(obj)
          cursor.current.foreach(
            cur => endLine = Some(cur.lineNumber)
          )
        case Some(line) if line.depth < rowDepth  =>
          continue = false
        case Some(_)                              =>
          continue = false
        case None                                 =>
          continue = false
      }
    }

    val table = rows.toVector
    assertExpectedCount(table.length, header.length, "tabular rows", options)
    if (options.strict) {
      for {
        start <- startLine
        end   <- endLine
      } {
        validateNoBlankLinesInRange(
          start,
          end,
          cursor.getBlankLines,
          options.strict,
          "tabular array"
        )
      }
      validateNoExtraTabularRows(cursor, rowDepth, header)
    }
    table
  }

  private def decodeListItem(
      cursor: LineCursor,
      baseDepth: Int,
      options: DecodeOptions
  ): JsonValue = {
    val line        = cursor.next().getOrElse(throw new NoSuchElementException("Expected list item"))
    val content     = line.content
    val emptyObject = JObj(VectorMap.empty)
    if (content == C.ListItemMarker) emptyObject
    else {
      val afterHyphen =
        if (content.startsWith(C.ListItemPrefix)) content.drop(C.ListItemPrefix.length)
        else
          throw DecodeError.Syntax(s"""Expected list item to start with "${C.ListItemPrefix}"""")

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
          .getOrElse(parsePrimitiveToken(afterHyphen))
      }
    }
  }

  private def decodeObjectFromListItem(
      firstLine: ParsedLine,
      cursor: LineCursor,
      baseDepth: Int,
      options: DecodeOptions
  ): JsonValue = {
    val afterHyphen                                      = firstLine.content.drop(C.ListItemPrefix.length)
    val KeyValueParse(firstKey, firstValue, followDepth) =
      decodeKeyValue(afterHyphen, cursor, baseDepth, options)
    var obj                                              = VectorMap(firstKey -> firstValue)
    var continue                                         = true

    while (continue && !cursor.atEnd) {
      cursor.peek match {
        case Some(line) if line.depth < followDepth =>
          continue = false
        case Some(line)
            if line.depth == followDepth && !line.content.startsWith(C.ListItemPrefix) =>
          cursor.advance()
          val KeyValueParse(k, v, _) = decodeKeyValue(line.content, cursor, followDepth, options)
          obj = obj.updated(k, v)
        case _                                      =>
          continue = false
      }
    }

    JObj(obj)
  }
}
