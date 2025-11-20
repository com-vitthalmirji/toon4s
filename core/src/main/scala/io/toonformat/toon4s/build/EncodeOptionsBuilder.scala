package io.toonformat.toon4s

// Phantom markers
sealed trait Missing

sealed trait Present

final class EncodeOptionsBuilder[HasIndent, HasDelimiter] private (
    private val indentOpt: Option[Int],
    private val delimiterOpt: Option[Delimiter],
    private val lengthMarker: Boolean,
) {

  /**
   * Set indent size with runtime validation.
   *
   * ==Runtime Validation Pattern==
   * While phantom types provide compile-time validation of required fields, runtime validation
   * ensures values are within acceptable ranges.
   *
   * @param n
   *   Number of spaces per indentation level
   * @return
   *   Builder with indent set
   *
   * Throws `java.lang.IllegalArgumentException` if indent is not positive.
   *
   * @example
   *   {{{
   * EncodeOptionsBuilder.empty.indent(2).delimiter(Delimiter.Comma).build
   * // Ok
   *
   * EncodeOptionsBuilder.empty.indent(0).delimiter(Delimiter.Comma).build
   * // Throws: java.lang.IllegalArgumentException("Indent must be positive, got: 0")
   *   }}}
   */
  def indent(n: Int): EncodeOptionsBuilder[Present, HasDelimiter] = {
    require(n > 0, s"Indent must be positive, got: $n")
    require(n <= 32, s"Indent must be <= 32 for readability, got: $n")
    new EncodeOptionsBuilder(Some(n), delimiterOpt, lengthMarker)
  }

  def delimiter(d: Delimiter): EncodeOptionsBuilder[HasIndent, Present] =
    new EncodeOptionsBuilder(indentOpt, Some(d), lengthMarker)

  def withLengthMarker(flag: Boolean): EncodeOptionsBuilder[HasIndent, HasDelimiter] =
    new EncodeOptionsBuilder(indentOpt, delimiterOpt, flag)

  def build(implicit ev1: HasIndent =:= Present, ev2: HasDelimiter =:= Present): EncodeOptions =
    EncodeOptions(indent = indentOpt.get, delimiter = delimiterOpt.get, lengthMarker = lengthMarker)

}

object EncodeOptionsBuilder {

  type IndentSet = Present

  type DelimiterSet = Present

  def empty: EncodeOptionsBuilder[Missing, Missing] =
    new EncodeOptionsBuilder(None, None, lengthMarker = false)

  def defaults: EncodeOptions = EncodeOptions()

}
