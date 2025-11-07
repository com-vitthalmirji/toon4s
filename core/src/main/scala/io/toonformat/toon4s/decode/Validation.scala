package io.toonformat.toon4s
package decode

import io.toonformat.toon4s.error.DecodeError
import io.toonformat.toon4s.{Constants => C, Delimiter}

private[decode] object Validation {
  def assertExpectedCount(
      actual: Int,
      expected: Int,
      itemType: String,
      options: DecodeOptions
  ): Unit = {
    if (options.strict && actual != expected) {
      throw DecodeError.Range(s"Expected $expected $itemType, but got $actual")
    }
  }

  def validateNoExtraListItems(cursor: LineCursor, itemDepth: Int, expectedCount: Int): Unit = {
    if (!cursor.atEnd) {
      cursor.peek match {
        case Some(next)
            if next.depth == itemDepth &&
              (next.content.startsWith(C.ListItemPrefix) || next.content == C.ListItemMarker) =>
          throw DecodeError.Range(s"Expected $expectedCount list array items, but found more")
        case _ => ()
      }
    }
  }

  def validateNoExtraTabularRows(
      cursor: LineCursor,
      rowDepth: Int,
      header: ArrayHeaderInfo
  ): Unit = {
    if (!cursor.atEnd) {
      cursor.peek match {
        case Some(next)
            if next.depth == rowDepth &&
              !next.content.startsWith(C.ListItemPrefix) &&
              isDataRow(next.content, header.delimiter) =>
          throw DecodeError.Range(s"Expected ${header.length} tabular rows, but found more")
        case _ => ()
      }
    }
  }

  def validateNoBlankLinesInRange(
      startLine: Int,
      endLine: Int,
      blankLines: Vector[BlankLine],
      strict: Boolean,
      context: String
  ): Unit = {
    if (strict) {
      val offending = blankLines.find(
        blank => blank.lineNumber > startLine && blank.lineNumber < endLine
      )
      offending.foreach {
        blank =>
          throw DecodeError.Syntax(
            s"Line ${blank.lineNumber}: Blank lines inside $context are not allowed in strict mode"
          )
      }
    }
  }

  private def isDataRow(content: String, delimiter: Delimiter): Boolean = {
    val colonPos     = content.indexOf(C.Colon)
    val delimiterPos = content.indexOf(delimiter.char)
    if (colonPos == -1) true
    else if (delimiterPos != -1 && delimiterPos < colonPos) true
    else false
  }
}
