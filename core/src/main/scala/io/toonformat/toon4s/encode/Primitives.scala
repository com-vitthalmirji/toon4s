package io.toonformat.toon4s
package encode

import io.toonformat.toon4s.JsonValue._
import io.toonformat.toon4s.{Constants => C, Delimiter}

private[toon4s] object Primitives {
  def encodePrimitive(p: JsonValue, delim: Delimiter): String = p match {
    case JNull      => C.NullLiteral
    case JBool(b)   => if (b) C.TrueLiteral else C.FalseLiteral
    case JNumber(n) => normalizeNumber(n)
    case JString(s) => encodeStringLiteral(s, delim)
    case other      => throw new IllegalArgumentException(s"Not a primitive: $other")
  }

  def encodeStringLiteral(s: String, delim: Delimiter): String = {
    if (isSafeUnquoted(s, delim)) s else "\"" + escapeString(s) + "\""
  }

  private def normalizeNumber(n: BigDecimal): String = {
    val normalized = n.bigDecimal.stripTrailingZeros.toPlainString
    if (normalized == "-0") "0" else normalized
  }

  def encodeKey(key: String): String = {
    if (isValidUnquotedKey(key)) key else "\"" + escapeString(key) + "\""
  }

  private val ValidKeyRegex = "^[A-Za-z_][A-Za-z0-9_.]*$".r

  def isValidUnquotedKey(key: String): Boolean = ValidKeyRegex.matches(key)

  def isSafeUnquoted(value: String, delim: Delimiter): Boolean = {
    val structuralChars = Set('"', '\\', '[', ']', '{', '}', '\n', '\r', '\t')
    val trimmed         = value.trim
    val passesBasicChecks =
      value.nonEmpty &&
        trimmed == value &&
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

  def escapeString(s: String): String = {
    val builder = new StringBuilder
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
}
