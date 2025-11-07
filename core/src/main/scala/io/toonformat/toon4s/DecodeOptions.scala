package io.toonformat.toon4s

/** Strictness level for TOON decoding, per TOON v1.4 §1.7.
  *
  * TOON specification defines strict mode as a boolean (strict vs non-strict). This sealed trait
  * provides type-safe pattern matching for the two modes.
  */
sealed trait Strictness

object Strictness {

  /** Strict mode (TOON v1.4 §14): Enforces all spec requirements:
    *   - Array length counts must match declared [N]
    *   - Indentation must be exact multiples of indentSize
    *   - No tabs in indentation
    *   - No blank lines inside arrays/tabular rows
    *   - All escape sequences must be valid
    */
  case object Strict extends Strictness

  /** Lenient mode: Relaxes validation, accepts malformed input when possible. Used for error
    * recovery or tolerant parsing scenarios.
    */
  case object Lenient extends Strictness
}

/** Options for decoding TOON documents.
  *
  * @param indent
  *   Number of spaces per indentation level (default: 2)
  * @param strictness
  *   Validation strictness level (default: Strict)
  */
final case class DecodeOptions(
    indent: Int = 2,
    strictness: Strictness = Strictness.Strict
)
