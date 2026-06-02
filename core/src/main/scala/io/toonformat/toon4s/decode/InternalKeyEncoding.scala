package io.toonformat.toon4s
package decode

/**
 * Encodes quoting metadata into object key strings for the decode pipeline.
 *
 * During decode, TOON distinguishes quoted keys (e.g. `"user.name": 1`) from unquoted keys (e.g.
 * `user: 1`). PathExpander must see this distinction to decide whether to expand dotted keys into
 * nested objects (spec §13.4).
 *
 * Encoding: a quoted key is stored as `Sentinel ++ rawKey`; an unquoted key is stored as-is.
 *
 * Why U+0001 is collision-free:
 *   - Unquoted keys match `^[A-Za-z_][A-Za-z0-9_.]*$` (spec §7.3) — U+0001 is not in that character
 *     class, so an unquoted key can never start with the sentinel character.
 *   - A quoted key whose decoded value starts with U+0001 is stored as `rest`; PathExpander
 *     strips exactly one sentinel and recovers `rest` correctly.
 *
 * This encoding is ONLY valid for JObj values produced by the decoder. User-constructed JObj values
 * must not pass through PathExpander directly.
 */
private[decode] object InternalKeyEncoding {

  private val Sentinel: Char = ''

  /** Encode a key and its quoting state into a stored key string. */
  def encode(key: String, quoted: Boolean): String =
    if (quoted) s"$Sentinel$key" else key

  /** Decode a stored key back into its raw value and quoting state. */
  def decode(stored: String): (String, Boolean) =
    if (stored.nonEmpty && stored.charAt(0) == Sentinel) (stored.substring(1), true)
    else (stored, false)

  /** True if the stored key was produced from a quoted key. */
  def isQuoted(stored: String): Boolean =
    stored.nonEmpty && stored.charAt(0) == Sentinel

}
