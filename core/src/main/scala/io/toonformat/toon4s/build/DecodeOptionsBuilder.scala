package io.toonformat.toon4s

// Phantom markers reused from EncodeOptionsBuilder (Missing/Present)
sealed trait DMissing

sealed trait DPresent

final class DecodeOptionsBuilder[HasIndent] private (
    private val indentOpt: Option[Int],
    private val strictness: Strictness,
) {

  /**
   * Set indent size with runtime validation.
   *
   * ==Runtime Validation Pattern==
   * Validates indent value at runtime to ensure it's within acceptable range.
   *
   * @param n
   *   Number of spaces per indentation level
   * @return
   *   Builder with indent set
   * @throws IllegalArgumentException
   *   if indent is not positive or too large
   *
   * @example
   *   {{{
   * DecodeOptionsBuilder.empty.indent(2).build
   * // Ok: DecodeOptions(indent = 2, strictness = Strict)
   *
   * DecodeOptionsBuilder.empty.indent(0).build
   * // Throws: IllegalArgumentException("Indent must be positive, got: 0")
   *   }}}
   */
  def indent(n: Int): DecodeOptionsBuilder[DPresent] = {
    require(n > 0, s"Indent must be positive, got: $n")
    require(n <= 32, s"Indent must be <= 32 for readability, got: $n")
    new DecodeOptionsBuilder(Some(n), strictness)
  }

  def strictness(s: Strictness): DecodeOptionsBuilder[HasIndent] =
    new DecodeOptionsBuilder(indentOpt, s)

  def strict(): DecodeOptionsBuilder[HasIndent] =
    new DecodeOptionsBuilder(indentOpt, Strictness.Strict)

  def lenient(): DecodeOptionsBuilder[HasIndent] =
    new DecodeOptionsBuilder(indentOpt, Strictness.Lenient)

  def build(implicit ev: HasIndent =:= DPresent): DecodeOptions =
    DecodeOptions(indent = indentOpt.get, strictness = strictness)

}

object DecodeOptionsBuilder {

  type IndentSet = DPresent

  def empty: DecodeOptionsBuilder[DMissing] =
    new DecodeOptionsBuilder(None, strictness = Strictness.Strict)

}
