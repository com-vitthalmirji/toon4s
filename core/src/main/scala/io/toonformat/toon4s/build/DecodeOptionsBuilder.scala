package io.toonformat.toon4s

// Phantom markers reused from EncodeOptionsBuilder (Missing/Present)
sealed trait DMissing
sealed trait DPresent

final class DecodeOptionsBuilder[HasIndent] private (
    private val indentOpt: Option[Int],
    private val strictness: Strictness
) {
  def indent(n: Int): DecodeOptionsBuilder[DPresent] =
    new DecodeOptionsBuilder(Some(n), strictness)

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
