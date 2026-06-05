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

  def encodeKey(key: String): String = {
    if (isValidUnquotedKey(key)) key else quoteAndEscape(key)
  }

  private def isAsciiLetter(c: Char): Boolean =
    (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')

  private def isAsciiDigit(c: Char): Boolean = c >= '0' && c <= '9'

  // Hand-rolled scan equivalent to "^[A-Za-z_][A-Za-z0-9_.]*$". Avoids a regex Matcher per key.
  def isValidUnquotedKey(key: String): Boolean = {
    if (key.isEmpty) false
    else {
      val first = key.charAt(0)
      if (!(isAsciiLetter(first) || first == '_')) false
      else {
        var i = 1
        var ok = true
        while (ok && i < key.length) {
          val c = key.charAt(i)
          if (!(isAsciiLetter(c) || isAsciiDigit(c) || c == '_' || c == '.')) ok = false
          i += 1
        }
        ok
      }
    }
  }

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

  // Hand-rolled scan equivalent to "^-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?$". Avoids a regex Matcher
  // per value on the encode hot path.
  private def isPlainNumeric(value: String): Boolean = {
    val n = value.length
    if (n == 0) false
    else {
      var i = 0
      if (value.charAt(0) == '-') i += 1
      val intStart = i
      while (i < n && isAsciiDigit(value.charAt(i))) i += 1
      if (i == intStart) false
      else {
        if (i < n && value.charAt(i) == '.') {
          i += 1
          val fracStart = i
          while (i < n && isAsciiDigit(value.charAt(i))) i += 1
          if (i == fracStart) return false
        }
        if (i < n && (value.charAt(i) == 'e' || value.charAt(i) == 'E')) {
          i += 1
          if (i < n && (value.charAt(i) == '+' || value.charAt(i) == '-')) i += 1
          val expStart = i
          while (i < n && isAsciiDigit(value.charAt(i))) i += 1
          if (i == expStart) return false
        }
        i == n
      }
    }
  }

  // Equivalent to "^0\d+$": a leading zero followed by one or more digits.
  private def isLeadingZeroNumber(value: String): Boolean = {
    val n = value.length
    if (n < 2 || value.charAt(0) != '0') false
    else {
      var i = 1
      var ok = true
      while (ok && i < n) {
        if (!isAsciiDigit(value.charAt(i))) ok = false
        i += 1
      }
      ok
    }
  }

  private def isNumericLike(value: String): Boolean =
    isPlainNumeric(value) || isLeadingZeroNumber(value)

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
