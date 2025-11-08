package io.toonformat.toon4s

/**
 * Strictness level for TOON decoding, per TOON v1.4 §1.7.
 *
 * TOON specification defines strict mode as a boolean (strict vs non-strict). This sealed trait
 * provides type-safe pattern matching for the two modes.
 */
sealed trait Strictness

object Strictness {

  /**
   * Strict mode (TOON v1.4 §14): Enforces all spec requirements:
   *   - Array length counts must match declared [N]
   *   - Indentation must be exact multiples of indentSize
   *   - No tabs in indentation
   *   - No blank lines inside arrays/tabular rows
   *   - All escape sequences must be valid
   */
  case object Strict extends Strictness

  /**
   * Lenient mode: Relaxes validation, accepts malformed input when possible. Used for error
   * recovery or tolerant parsing scenarios.
   */
  case object Lenient extends Strictness

}

/**
 * Configuration options for decoding TOON documents.
 *
 * ==Usage==
 * {{{
 * import io.toonformat.toon4s._
 *
 * // Strict mode with default limits (security-conscious)
 * val strict = DecodeOptions()
 *
 * // Lenient mode for tolerant parsing
 * val lenient = DecodeOptions(strictness = Strictness.Lenient)
 *
 * // Custom limits for large data
 * val custom = DecodeOptions(
 *   maxDepth = Some(5000),
 *   maxArrayLength = Some(1000000)
 * )
 *
 * Toon.decode(input, lenient)
 * }}}
 *
 * ==Security considerations==
 * The default limits prevent DoS attacks from maliciously crafted inputs:
 *   - Deep nesting can cause stack overflow
 *   - Large arrays/strings can exhaust memory Set to `None` only when processing trusted inputs.
 *
 * @param indent
 *   Number of spaces per indentation level (default: 2). Must match the indent used during
 *   encoding.
 * @param strictness
 *   Validation strictness level (default: Strict). See [[Strictness]] for details.
 * @param maxDepth
 *   Maximum nesting depth (default: Some(1000), None = no limit). Prevents stack overflow from
 *   deeply nested structures.
 * @param maxArrayLength
 *   Maximum array length (default: Some(100000), None = no limit). Prevents memory exhaustion from
 *   extremely large arrays.
 * @param maxStringLength
 *   Maximum string length (default: Some(1000000), None = no limit). Prevents memory exhaustion
 *   from extremely large strings.
 *
 * @see
 *   [[Strictness]] for strictness modes
 * @see
 *   [[Toon.decode]] for decoding with options
 */
final case class DecodeOptions(
    indent: Int = 2,
    strictness: Strictness = Strictness.Strict,
    maxDepth: Option[Int] = Some(1000),
    maxArrayLength: Option[Int] = Some(100000),
    maxStringLength: Option[Int] = Some(1000000),
) {

  // Runtime validation for all parameters
  require(indent > 0, s"indent must be positive, got: $indent")

  require(indent <= 32, s"indent must be <= 32 for readability, got: $indent")

  maxDepth.foreach {
    depth => require(depth > 0, s"maxDepth must be positive if specified, got: $depth")
  }

  maxArrayLength.foreach {
    len => require(len > 0, s"maxArrayLength must be positive if specified, got: $len")
  }

  maxStringLength.foreach {
    len => require(len > 0, s"maxStringLength must be positive if specified, got: $len")
  }

}
