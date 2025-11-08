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
 * @param lengthMarker
 *   Whether to emit [N] length markers for arrays (default: false). When true, arrays are prefixed
 *   with their length (e.g., "arr[3]: 1,2,3"). Useful for validation but adds verbosity.
 *
 * @see
 *   [[Delimiter]] for delimiter options
 * @see
 *   [[Toon.encode]] for encoding with options
 */
final case class EncodeOptions(
    indent: Int = 2,
    delimiter: Delimiter = Delimiter.Comma,
    lengthMarker: Boolean = false,
) {

  require(indent > 0, s"indent must be positive, got: $indent")

  require(indent <= 32, s"indent must be <= 32 for readability, got: $indent")

}
