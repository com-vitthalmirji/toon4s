package io.toonformat.toon4s

/** Delimiter character for inline arrays and tabular data in TOON format.
  *
  * The delimiter separates values in inline arrays and columns in tabular array representations.
  *
  * ==Usage==
  * {{{
  * import io.toonformat.toon4s._
  *
  * // Use comma delimiter (default)
  * val commaOpts = EncodeOptions(delimiter = Delimiter.Comma)
  * // Produces: arr[3]: 1,2,3
  *
  * // Use tab delimiter
  * val tabOpts = EncodeOptions(delimiter = Delimiter.Tab)
  * // Produces: arr[3]: 1\t2\t3
  *
  * // Use pipe delimiter
  * val pipeOpts = EncodeOptions(delimiter = Delimiter.Pipe)
  * // Produces: arr[3]: 1|2|3
  * }}}
  *
  * @param char
  *   The delimiter character
  *
  * @see
  *   [[EncodeOptions]] for encoding configuration
  */
sealed abstract class Delimiter(val char: Char)

object Delimiter {

  /** Comma delimiter (`,`). Default and most common choice.
    *
    * @example
    *   {{{
    * arr[3]: 1,2,3
    * users[2]{id,name}:
    *   1,Alice
    *   2,Bob
    *   }}}
    */
  case object Comma extends Delimiter(',')

  /** Tab delimiter (`\t`). Useful for TSV-like tabular data.
    *
    * @example
    *   {{{
    * arr[3]: 1\t2\t3
    * users[2]{id\tname}:
    *   1\tAlice
    *   2\tBob
    *   }}}
    */
  case object Tab extends Delimiter('\t')

  /** Pipe delimiter (`|`). Common in legacy formats and easy to read.
    *
    * @example
    *   {{{
    * arr[3]: 1|2|3
    * users[2]{id|name}:
    *   1|Alice
    *   2|Bob
    *   }}}
    */
  case object Pipe extends Delimiter('|')

  /** All available delimiter values. */
  val values: List[Delimiter] = List(Comma, Tab, Pipe)
}
