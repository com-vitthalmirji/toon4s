package io.toonformat.toon4s
package decode
package parsers

import io.toonformat.toon4s.Delimiter

/**
 * Information parsed from an array header.
 *
 * ==Design: Product type (case class)==
 *
 * Immutable data structure representing all information from a TOON array header.
 *
 * @param key
 *   Optional key name (for object fields with array values)
 * @param length
 *   Declared array length from `[N]`
 * @param delimiter
 *   Delimiter character for values (comma, tab, or pipe)
 * @param fields
 *   Field names for tabular arrays from `{field1,field2}`
 * @param hasLengthMarker
 *   Whether the header had a `#` length marker
 *
 * @example
 *   {{{
 * // arr[3]: 1,2,3
 * ArrayHeaderInfo(Some("arr"), 3, Delimiter.Comma, Nil, false)
 *
 * // users[2]{id,name}:
 * ArrayHeaderInfo(Some("users"), 2, Delimiter.Comma, List("id", "name"), false)
 *
 * // [#5|]:
 * ArrayHeaderInfo(None, 5, Delimiter.Pipe, Nil, true)
 *   }}}
 */
final case class ArrayHeaderInfo(
    key: Option[String],
    length: Int,
    delimiter: Delimiter,
    fields: List[String],
    hasLengthMarker: Boolean,
)
