package io.toonformat.toon4s

// Phantom markers
sealed trait Missing

sealed trait Present

final class EncodeOptionsBuilder[HasIndent, HasDelimiter] private (
    private val indentOpt: Option[Int],
    private val delimiterOpt: Option[Delimiter],
    private val keyFolding: KeyFolding,
    private val flattenDepth: Int,
    private val lengthMarker: Boolean,
) {

  def indent(n: Int): EncodeOptionsBuilder[Present, HasDelimiter] = {
    require(n > 0, s"Indent must be positive, got: $n")
    require(n <= 32, s"Indent must be <= 32 for readability, got: $n")
    new EncodeOptionsBuilder(Some(n), delimiterOpt, keyFolding, flattenDepth, lengthMarker)
  }

  def delimiter(d: Delimiter): EncodeOptionsBuilder[HasIndent, Present] =
    new EncodeOptionsBuilder(indentOpt, Some(d), keyFolding, flattenDepth, lengthMarker)

  def withKeyFolding(mode: KeyFolding): EncodeOptionsBuilder[HasIndent, HasDelimiter] =
    new EncodeOptionsBuilder(indentOpt, delimiterOpt, mode, flattenDepth, lengthMarker)

  def withFlattenDepth(n: Int): EncodeOptionsBuilder[HasIndent, HasDelimiter] = {
    require(n >= 0, s"flattenDepth must be non-negative, got: $n")
    new EncodeOptionsBuilder(indentOpt, delimiterOpt, keyFolding, n, lengthMarker)
  }

  def withLengthMarker(enabled: Boolean): EncodeOptionsBuilder[HasIndent, HasDelimiter] =
    new EncodeOptionsBuilder(indentOpt, delimiterOpt, keyFolding, flattenDepth, enabled)

  def build(implicit ev1: HasIndent =:= Present, ev2: HasDelimiter =:= Present): EncodeOptions =
    EncodeOptions(
      indent = indentOpt.get,
      delimiter = Delimiter.withLengthMarker(delimiterOpt.get, lengthMarker),
      keyFolding = keyFolding,
      flattenDepth = flattenDepth,
    )

}

object EncodeOptionsBuilder {

  type IndentSet = Present

  type DelimiterSet = Present

  def empty: EncodeOptionsBuilder[Missing, Missing] =
    new EncodeOptionsBuilder(
      None,
      None,
      keyFolding = KeyFolding.Off,
      flattenDepth = Int.MaxValue,
      lengthMarker = false,
    )

  def defaults: EncodeOptions = EncodeOptions()

}
