package io.toonformat.toon4s
package encode

import io.toonformat.toon4s.{Constants => C, Delimiter}
import io.toonformat.toon4s.JsonValue._

private[toon4s] object Primitives {

  // Hoisted to object level to avoid recreation per call
  // Hot path optimization - this Set was being created millions of times during encoding
  private val structuralChars = Set('"', '\\', '[', ']', '{', '}', '\n', '\r', '\t')

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

  /**
   * Quote and escape a string in a single pass, avoiding intermediate allocations.
   *
   * Merges quoting and escaping into one operation to eliminate intermediate StringBuilder
   * allocations. Hot path optimization - called for every string value that needs quoting during
   * encoding.
   *
   * @param s
   *   The string to quote and escape
   * @return
   *   Quoted and escaped string
   */
  def quoteAndEscape(s: String): String = {
    val builder = new StringBuilder(s.length + 18)
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

  private def normalizeNumber(n: BigDecimal): String = {
    val normalized = n.bigDecimal.stripTrailingZeros.toPlainString
    if (normalized == "-0") "0" else normalized
  }

  // Raw-value primitive formatting. Lets typed callers (such as the Spark integration) emit
  // canonical TOON tokens straight from primitive values without first wrapping them in a
  // JsonValue. Output is identical to encodePrimitive for the same value.
  //
  // None of these allocate a BigDecimal.

  val nullToken: String = C.NullLiteral

  def formatBoolean(b: Boolean): String = if (b) C.TrueLiteral else C.FalseLiteral

  def formatLong(n: Long): String = java.lang.Long.toString(n)

  def formatBigInt(n: BigInt): String = n.toString

  /**
   * Format a double in canonical TOON form (plain decimal, no exponent, no trailing zeros) without
   * allocating a BigDecimal. Java's Double.toString already produces the shortest round-trip
   * digits; this reformats those digits in one pass into a single buffer, so the output is
   * byte-identical to BigDecimal(d).stripTrailingZeros.toPlainString.
   */
  def formatDouble(d: Double): String =
    if (d == 0.0) "0" // covers +0.0 and -0.0
    else {
      val sb = new java.lang.StringBuilder(24)
      appendCanonicalDouble(sb, d)
      sb.toString
    }

  private def appendCanonicalDouble(sb: java.lang.StringBuilder, d: Double): Unit = {
    val s = java.lang.Double.toString(d)
    val n = s.length
    var start = 0
    if (s.charAt(0) == '-') {
      sb.append('-')
      start = 1
    }
    val dot = s.indexOf('.', start)
    val eIdx = s.indexOf('E', start)
    val intEnd = dot
    val fracStart = dot + 1
    val fracEnd = if (eIdx < 0) n else eIdx
    val exp = if (eIdx < 0) 0 else parseExp(s, eIdx + 1, n)
    val intLen = intEnd - start
    val fracLen = fracEnd - fracStart
    val totalDigits = intLen + fracLen
    // The decimal point sits after `pointPos` significant digits once the exponent is applied.
    val pointPos = intLen + exp

    def digitAt(k: Int): Char =
      if (k < intLen) s.charAt(start + k) else s.charAt(fracStart + (k - intLen))

    if (pointPos <= 0) {
      // All digits are fractional: 0.00...digits, with trailing zeros stripped.
      var last = totalDigits - 1
      while (last >= 0 && digitAt(last) == '0') last -= 1
      sb.append('0').append('.')
      var z = 0
      while (z < -pointPos) { sb.append('0'); z += 1 }
      var k = 0
      while (k <= last) { sb.append(digitAt(k)); k += 1 }
    } else if (pointPos >= totalDigits) {
      // Integer value: all digits plus trailing zeros from the exponent.
      var k = 0
      while (k < totalDigits) { sb.append(digitAt(k)); k += 1 }
      var z = 0
      while (z < pointPos - totalDigits) { sb.append('0'); z += 1 }
    } else {
      // Mixed: integer part then fractional part with trailing zeros stripped.
      var k = 0
      while (k < pointPos) { sb.append(digitAt(k)); k += 1 }
      var last = totalDigits - 1
      while (last >= pointPos && digitAt(last) == '0') last -= 1
      if (last >= pointPos) {
        sb.append('.')
        var j = pointPos
        while (j <= last) { sb.append(digitAt(j)); j += 1 }
      }
    }
  }

  private def parseExp(s: String, from: Int, to: Int): Int = {
    var i = from
    var neg = false
    if (s.charAt(i) == '-') { neg = true; i += 1 }
    var v = 0
    while (i < to) {
      v = v * 10 + (s.charAt(i) - '0')
      i += 1
    }
    if (neg) -v else v
  }

  def formatBigDecimal(n: BigDecimal): String = normalizeNumber(n)

  def encodeKey(key: String): String = {
    if (isValidUnquotedKey(key)) key else quoteAndEscape(key)
  }

  private val ValidKeyRegex = "^[A-Za-z_][A-Za-z0-9_.]*$".r

  def isValidUnquotedKey(key: String): Boolean = ValidKeyRegex.matches(key)

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
    !value.exists(structuralChars.contains) &&
    !value.exists(_ < ' ')
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
