package io.toonformat.toon4s
package encode

import io.toonformat.toon4s.{Constants => C, Delimiter}
import io.toonformat.toon4s.JsonValue._

private[toon4s] object Primitives {

  def encodePrimitive(p: JsonValue, delim: Delimiter): String = p match {
  case JNull      => C.NullLiteral
  case JBool(b)   => if (b) C.TrueLiteral else C.FalseLiteral
  case JNumber(n) => normalizeNumber(n)
  case JString(s) => encodeStringLiteral(s, delim)
  case other      => throw new IllegalArgumentException(s"Not a primitive: $other")
  }

  def encodeStringLiteral(s: String, delim: Delimiter): String = {
    if (isSafeUnquoted(s, delim)) s else quoteAndEscape(s)
  }

  private def normalizeNumber(n: BigDecimal): String = {
    val normalized = n.bigDecimal.stripTrailingZeros.toPlainString
    if (normalized == "-0") "0" else normalized
  }

  def encodeKey(key: String): String = {
    if (isValidUnquotedKey(key)) key else quoteAndEscape(key)
  }

  private val ValidKeyRegex = "^[A-Za-z_][A-Za-z0-9_.]*$".r

  def isValidUnquotedKey(key: String): Boolean = ValidKeyRegex.matches(key)

  val structuralChars = Set('"', '\\', '[', ']', '{', '}', '\n', '\r', '\t')

  def isSafeUnquoted(value: String, delim: Delimiter): Boolean = {
    val passesBasicChecks =
      value.nonEmpty &&
        !Character.isWhitespace(value.charAt(0)) &&
        !Character.isWhitespace(value.charAt(value.length - 1)) &&
        !value.contains(':') &&
        !value.contains(delim.char) &&
        !value.startsWith(C.ListItemMarker)
    passesBasicChecks &&
    !isBooleanOrNull(value) &&
    !isNumericLike(value) &&
    !value.exists(structuralChars.contains)
  }

  private def isBooleanOrNull(value: String): Boolean =
    value == C.TrueLiteral || value == C.FalseLiteral || value == C.NullLiteral

  private val NumericLikePattern = "^-?\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?$".r

  private val LeadingZeroPattern = "^0\\d+$".r

  private def isNumericLike(value: String): Boolean =
    NumericLikePattern.matches(value) || LeadingZeroPattern.matches(value)

  /**
   * Escape a string by converting special characters to escape sequences.
   *
   * ==Pure Function - Virtual Thread Friendly==
   * Uses local StringBuilder instead of ThreadLocal for compatibility with Java virtual threads
   * (Project Loom).
   *
   * ThreadLocal can cause issues with virtual threads because:
   *   - Virtual threads are cheap and numerous
   *   - ThreadLocal creates one value per thread
   *   - Can lead to memory leaks with many virtual threads
   *
   * ==Performance Strategy==
   * Pre-allocates StringBuilder with estimated capacity to minimize resizing.
   *
   * @param s
   *   The string to escape
   * @return
   *   Escaped string with special characters converted
   *
   * @example
   *   {{{
   * escapeString("hello\nworld")  // "hello\\nworld"
   * escapeString("say \"hi\"")    // "say \\\"hi\\\""
   *   }}}
   */
  def escapeString(s: String): String = {
    // Pre-allocate with estimated capacity (most strings don't need escaping)
    // This reduces allocations without ThreadLocal complexity
    val builder = new StringBuilder(s.length + 16)
    s.foreach {
      case '\\'             => builder.append("\\\\")
      case '"'              => builder.append("\\\"")
      case '\n'             => builder.append("\\n")
      case '\r'             => builder.append("\\r")
      case '\t'             => builder.append("\\t")
      case c if c.isControl => builder.append(f"\\u${c.toInt}%04x")
      case c                => builder.append(c)
    }
    builder.result()
  }

  /** Quote and escape a string in one pass. */
  def quoteAndEscape(s: String): String = {
    val builder = new StringBuilder(s.length + 18) // +2 for quotes, +16 for escapes
    builder.append('"')
    s.foreach {
      case '\\'             => builder.append("\\\\")
      case '"'              => builder.append("\\\"")
      case '\n'             => builder.append("\\n")
      case '\r'             => builder.append("\\r")
      case '\t'             => builder.append("\\t")
      case c if c.isControl => builder.append(f"\\u${c.toInt}%04x")
      case c                => builder.append(c)
    }
    builder.append('"')
    builder.result()
  }

  // Writer-based primitive emission (avoids intermediate strings)
  def writePrimitive(p: JsonValue, delim: Delimiter, out: java.io.Writer): Unit = p match {
  case JNull      => out.write(C.NullLiteral)
  case JBool(b)   => if (b) out.write(C.TrueLiteral) else out.write(C.FalseLiteral)
  case JNumber(n) => out.write(normalizeNumber(n))
  case JString(s) => writeStringLiteral(s, delim, out)
  case other      => throw new IllegalArgumentException(s"Not a primitive: $other")
  }

  def writeStringLiteral(s: String, delim: Delimiter, out: java.io.Writer): Unit = {
    if (isSafeUnquoted(s, delim)) out.write(s)
    else {
      out.write('"')
      writeEscaped(s, out)
      out.write('"')
    }
  }

  private def writeEscaped(s: String, out: java.io.Writer): Unit = {
    var i = 0
    while (i < s.length) {
      s.charAt(i) match {
      case '\\'             => out.write("\\\\")
      case '"'              => out.write("\\\"")
      case '\n'             => out.write("\\n")
      case '\r'             => out.write("\\r")
      case '\t'             => out.write("\\t")
      case c if c.isControl => out.write(f"\\u${c.toInt}%04x")
      case c                => out.write(c)
      }
      i += 1
    }
  }

}
