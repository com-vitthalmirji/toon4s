package io.toonformat.toon4s
package decode

import io.toonformat.toon4s.error.DecodeError
import io.toonformat.toon4s.{Constants => C, Delimiter, Strictness}

private[decode] object Validation {

  private def enforceIfStrict(msg: String, toErr: String => DecodeError)(implicit
      strictness: Strictness
  ): Unit =
    strictness match {
      case Strictness.Strict  => throw toErr(msg)
      case Strictness.Lenient => () // Silently accept
    }

  def assertExpectedCount(
      actual: Int,
      expected: Int,
      itemType: String
  )(implicit strictness: Strictness): Unit =
    if (actual != expected)
      enforceIfStrict(s"Expected $expected $itemType, but got $actual", DecodeError.Range.apply)

  def validateNoExtraListItems(cursor: LineCursor, itemDepth: Int, expectedCount: Int)(implicit
      strictness: Strictness
  ): Unit =
    if (!cursor.atEnd)
      cursor.peek match {
        case Some(next)
            if next.depth == itemDepth && (next.content.startsWith(
              C.ListItemPrefix
            ) || next.content == C.ListItemMarker) =>
          enforceIfStrict(
            s"Expected $expectedCount list array items, but found more",
            DecodeError.Range.apply
          )
        case _ => ()
      }

  def validateNoExtraTabularRows(
      cursor: LineCursor,
      rowDepth: Int,
      header: ArrayHeaderInfo
  )(implicit strictness: Strictness): Unit =
    if (!cursor.atEnd)
      cursor.peek match {
        case Some(next)
            if next.depth == rowDepth && !next.content
              .startsWith(C.ListItemPrefix) && isDataRow(next.content, header.delimiter) =>
          enforceIfStrict(
            s"Expected ${header.length} tabular rows, but found more",
            DecodeError.Range.apply
          )
        case _ => ()
      }

  def validateNoBlankLinesInRange(
      startLine: Int,
      endLine: Int,
      blankLines: Vector[BlankLine],
      context: String
  )(implicit strictness: Strictness): Unit =
    blankLines
      .find(
        blank => blank.lineNumber > startLine && blank.lineNumber < endLine
      )
      .foreach(
        blank =>
          enforceIfStrict(
            s"Line ${blank.lineNumber}: Blank lines inside $context are not allowed",
            DecodeError.Syntax.apply
          )
      )

  private def isDataRow(content: String, delimiter: Delimiter): Boolean = {
    val colonPos     = content.indexOf(C.Colon)
    val delimiterPos = content.indexOf(delimiter.char)
    if (colonPos == -1) true
    else if (delimiterPos != -1 && delimiterPos < colonPos) true
    else false
  }
}
