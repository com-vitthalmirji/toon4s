package io.toonformat.toon4s

/**
 * Configuration options for encoding values to TOON format.
 *
 * ==Usage==
 * {{{
 * import io.toonformat.toon4s._
 *
 * // Default options (2-space indent, comma delimiter)
 * val default = EncodeOptions()
 *
 * // Custom indent and tab delimiter
 * val custom = EncodeOptions(indent = 4, delimiter = Delimiter.Tab)
 *
 * Toon.encode(data, custom)
 * }}}
 *
 * @param indent
 *   Number of spaces per indentation level (default: 2). Must be positive.
 * @param delimiter
 *   Delimiter character for inline arrays and tabular data (default: Comma). See [[Delimiter]] for
 *   options.
 * @param keyFolding
 *   Optional dotted-path key folding (default: Off). When set to [[KeyFolding.Safe]], chains of
 *   single-key objects that satisfy IdentifierSegment rules are folded into `a.b` style keys.
 * @param flattenDepth
 *   Maximum number of segments to fold when [[KeyFolding.Safe]] is enabled (default: unlimited).
 *
 * @see
 *   [[Delimiter]] for delimiter options
 * @see
 *   [[Toon.encode]] for encoding with options
 */
final case class EncodeOptions(
    indent: Int = 2,
    delimiter: Delimiter = Delimiter.Comma,
    keyFolding: KeyFolding = KeyFolding.Off,
    flattenDepth: Int = Int.MaxValue,
) {

  require(indent > 0, s"indent must be positive, got: $indent")

  require(indent <= 32, s"indent must be <= 32 for readability, got: $indent")

  require(flattenDepth >= 0, s"flattenDepth must be non-negative, got: $flattenDepth")

}

sealed trait KeyFolding

object KeyFolding {

  case object Off extends KeyFolding

  case object Safe extends KeyFolding

}
