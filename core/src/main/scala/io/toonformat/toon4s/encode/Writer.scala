package io.toonformat.toon4s
package encode

final class LineWriter(indentSize: Int) {
  private val builder = new StringBuilder
  private var first   = true

  private def pad(depth: Int): Unit = {
    var i = 0
    while (i < depth * indentSize) {
      builder.append(' ')
      i += 1
    }
  }

  def push(depth: Int, line: String): Unit = {
    if (!first) builder.append('\n') else first = false
    pad(depth)
    builder.append(line)
  }

  def pushListItem(depth: Int, line: String): Unit = {
    if (!first) builder.append('\n') else first = false
    pad(depth)
    builder.append("- ")
    builder.append(line)
  }

  override def toString: String = builder.result()
}
