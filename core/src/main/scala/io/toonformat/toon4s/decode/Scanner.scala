package io.toonformat.toon4s
package decode

import io.toonformat.toon4s.error.DecodeError
import io.toonformat.toon4s.{Constants => C}

final case class ParsedLine(raw: String, depth: Int, indent: Int, content: String, lineNumber: Int)
final case class BlankLine(lineNumber: Int, indent: Int, depth: Int)
final case class ScanResult(lines: Vector[ParsedLine], blanks: Vector[BlankLine])

final class LineCursor(
    private val lines: Vector[ParsedLine],
    private val blanks: Vector[BlankLine]
) {
  private var idx = 0

  def peek: Option[ParsedLine] = lines.lift(idx)

  def next(): Option[ParsedLine] = {
    val cur = peek
    idx += 1
    cur
  }

  def current: Option[ParsedLine] = if (idx > 0) Some(lines(idx - 1)) else None

  def atEnd: Boolean = idx >= lines.length

  def length: Int = lines.length

  def peekAtDepth(target: Int): Option[ParsedLine] = peek.filter(_.depth == target)

  def hasMoreAtDepth(target: Int): Boolean = peekAtDepth(target).isDefined

  def getBlankLines: Vector[BlankLine] = blanks

  def advance(): Unit = idx += 1
}

object Scanner {
  import java.io.BufferedReader
  import java.io.Reader

  def toParsedLines(src: String, indentSize: Int, strict: Boolean): ScanResult = {
    if (src.trim.isEmpty) ScanResult(Vector.empty, Vector.empty)
    else {
      val rawLines = src.split("\n", -1).toVector
      var i        = 0
      val out      = Vector.newBuilder[ParsedLine]
      val blanks   = Vector.newBuilder[BlankLine]

      while (i < rawLines.length) {
        val raw     = rawLines(i)
        val lineNo  = i + 1
        var indent  = 0
        while (indent < raw.length && raw.charAt(indent) == C.Space) {
          indent += 1
        }
        val content = raw.substring(indent)
        if (content.trim.isEmpty) {
          val depth = indent / indentSize
          blanks += BlankLine(lineNo, indent, depth)
        } else {
          val depth = indent / indentSize
          if (strict) {
            val ws = raw.takeWhile(
              ch => ch == C.Space || ch == C.Tab
            )
            if (ws.contains(C.Tab)) {
              throw DecodeError.Syntax(
                s"Line $lineNo: Tabs are not allowed in indentation in strict mode"
              )
            }
            if (indent > 0 && indent % indentSize != 0) {
              throw DecodeError.Syntax(
                s"Line $lineNo: Indentation must be exact multiple of $indentSize, but found $indent spaces"
              )
            }
          }
          out += ParsedLine(raw, depth, indent, content, lineNo)
        }
        i += 1
      }

      ScanResult(out.result(), blanks.result())
    }
  }

  def toParsedLines(reader: Reader, indentSize: Int, strict: Boolean): ScanResult = {
    val br     = new BufferedReader(reader)
    val out    = Vector.newBuilder[ParsedLine]
    val blanks = Vector.newBuilder[BlankLine]
    var lineNo = 0
    var done   = false
    while (!done) {
      val raw = br.readLine()
      if (raw == null) done = true
      else {
        lineNo += 1
        var indent  = 0
        while (indent < raw.length && raw.charAt(indent) == C.Space) indent += 1
        val content = raw.substring(indent)
        if (content.trim.isEmpty) {
          val depth = indent / indentSize
          blanks += BlankLine(lineNo, indent, depth)
        } else {
          val depth = indent / indentSize
          if (strict) {
            val ws = raw.takeWhile(
              ch => ch == C.Space || ch == C.Tab
            )
            if (ws.contains(C.Tab))
              throw DecodeError.Syntax(
                s"Line $lineNo: Tabs are not allowed in indentation in strict mode"
              )
            if (indent > 0 && indent % indentSize != 0)
              throw DecodeError.Syntax(
                s"Line $lineNo: Indentation must be exact multiple of $indentSize, but found $indent spaces"
              )
          }
          out += ParsedLine(raw, depth, indent, content, lineNo)
        }
      }
    }
    ScanResult(out.result(), blanks.result())
  }
}
