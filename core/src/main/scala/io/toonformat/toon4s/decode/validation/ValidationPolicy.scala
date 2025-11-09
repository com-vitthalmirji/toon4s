package io.toonformat.toon4s
package decode
package validation

import io.toonformat.toon4s.error.DecodeError

/**
 * Validation policy for TOON decoding.
 *
 * ==Design: Strategy + Policy pattern==
 *
 * This trait encapsulates validation rules, allowing different strictness levels to be implemented
 * as separate policies. Each policy decides whether to:
 *   - Enforce validation (throw error)
 *   - Accept with warning (log/ignore)
 *   - Accept silently
 *
 * ==Testability==
 * Policies can be tested in isolation without needing full parsing infrastructure.
 *
 * @example
 *   {{{
 * val policy = StrictValidationPolicy
 * policy.validateArrayCount(actual = 3, expected = 5, "items") // throws
 *
 * val lenient = LenientValidationPolicy
 * lenient.validateArrayCount(actual = 3, expected = 5, "items") // accepts
 *   }}}
 */
sealed trait ValidationPolicy {

  /**
   * Validate that actual count matches expected count.
   *
   * @param actual
   *   Actual number of items found
   * @param expected
   *   Expected number of items
   * @param itemType
   *   Description of what is being counted (e.g., "array items", "table rows")
   * @throws io.toonformat.toon4s.error.DecodeError.Range
   *   if validation fails and policy is strict
   */
  def validateArrayCount(actual: Int, expected: Int, itemType: String): Unit

  /**
   * Validate that no extra items exist beyond expected count.
   *
   * @param hasExtra
   *   Whether extra items were detected
   * @param expected
   *   Expected count
   * @param itemType
   *   Description of items
   * @throws io.toonformat.toon4s.error.DecodeError.Range
   *   if validation fails and policy is strict
   */
  def validateNoExtraItems(hasExtra: Boolean, expected: Int, itemType: String): Unit

  /**
   * Validate that no blank lines exist in a range.
   *
   * @param hasBlankLines
   *   Whether blank lines were detected
   * @param context
   *   Description of the context (e.g., "list array", "tabular array")
   * @throws io.toonformat.toon4s.error.DecodeError.Syntax
   *   if validation fails and policy is strict
   */
  def validateNoBlankLines(hasBlankLines: Boolean, context: String): Unit

  /**
   * Validate nesting depth limit.
   *
   * @param currentDepth
   *   Current nesting level
   * @param maxDepth
   *   Maximum allowed depth
   * @throws io.toonformat.toon4s.error.DecodeError.Range
   *   if validation fails
   */
  def validateDepth(currentDepth: Int, maxDepth: Option[Int]): Unit

  /**
   * Validate array length limit.
   *
   * @param length
   *   Array length
   * @param maxLength
   *   Maximum allowed length
   * @throws io.toonformat.toon4s.error.DecodeError.Range
   *   if validation fails
   */
  def validateArrayLength(length: Int, maxLength: Option[Int]): Unit

  /**
   * Validate string length limit.
   *
   * @param length
   *   String length
   * @param maxLength
   *   Maximum allowed length
   * @throws io.toonformat.toon4s.error.DecodeError.Syntax
   *   if validation fails
   */
  def validateStringLength(length: Int, maxLength: Option[Int]): Unit

}

/**
 * Strict validation policy - enforces all rules.
 *
 * ==Design: Singleton Object==
 *
 * This policy throws errors on any validation failure, ensuring strict conformance to the TOON
 * specification.
 *
 * Used when data integrity is critical.
 */
case object StrictValidationPolicy extends ValidationPolicy {

  def validateArrayCount(actual: Int, expected: Int, itemType: String): Unit = {
    if (actual != expected) {
      throw DecodeError.Range(s"Expected $expected $itemType, but got $actual")
    }
  }

  def validateNoExtraItems(hasExtra: Boolean, expected: Int, itemType: String): Unit = {
    if (hasExtra) {
      throw DecodeError.Range(s"Expected $expected $itemType, but found more")
    }
  }

  def validateNoBlankLines(hasBlankLines: Boolean, context: String): Unit = {
    if (hasBlankLines) {
      throw DecodeError.Syntax(s"Blank lines inside $context are not allowed")
    }
  }

  def validateDepth(currentDepth: Int, maxDepth: Option[Int]): Unit = {
    maxDepth.foreach {
      limit =>
        if (currentDepth > limit) {
          throw DecodeError.Range(s"Exceeded maximum nesting depth of $limit")
        }
    }
  }

  def validateArrayLength(length: Int, maxLength: Option[Int]): Unit = {
    maxLength.foreach {
      limit =>
        if (length > limit) {
          throw DecodeError.Range(s"Exceeded maximum array length of $limit")
        }
    }
  }

  def validateStringLength(length: Int, maxLength: Option[Int]): Unit = {
    maxLength.foreach {
      limit =>
        if (length > limit) {
          throw DecodeError.Syntax(s"Exceeded maximum string length of $limit")
        }
    }
  }

}

/**
 * Lenient validation policy - accepts most inputs.
 *
 * ==Design: Singleton object==
 *
 * This policy silently accepts validation failures for non-critical rules, while still enforcing
 * security limits (depth, size).
 *
 * Used when parsing untrusted or malformed data that needs best-effort recovery.
 */
case object LenientValidationPolicy extends ValidationPolicy {

  def validateArrayCount(actual: Int, expected: Int, itemType: String): Unit = {
    // Accept silently - allow count mismatches
    ()
  }

  def validateNoExtraItems(hasExtra: Boolean, expected: Int, itemType: String): Unit = {
    // Accept silently - allow extra items
    ()
  }

  def validateNoBlankLines(hasBlankLines: Boolean, context: String): Unit = {
    // Accept silently - allow blank lines
    ()
  }

  // Security limits are always enforced, even in lenient mode
  def validateDepth(currentDepth: Int, maxDepth: Option[Int]): Unit = {
    maxDepth.foreach {
      limit =>
        if (currentDepth > limit) {
          throw DecodeError.Range(s"Exceeded maximum nesting depth of $limit")
        }
    }
  }

  def validateArrayLength(length: Int, maxLength: Option[Int]): Unit = {
    maxLength.foreach {
      limit =>
        if (length > limit) {
          throw DecodeError.Range(s"Exceeded maximum array length of $limit")
        }
    }
  }

  def validateStringLength(length: Int, maxLength: Option[Int]): Unit = {
    maxLength.foreach {
      limit =>
        if (length > limit) {
          throw DecodeError.Syntax(s"Exceeded maximum string length of $limit")
        }
    }
  }

}

/**
 * Factory for creating validation policies.
 *
 * ==Design: Factory pattern==
 */
object ValidationPolicy {

  /**
   * Create a validation policy from strictness level.
   *
   * @param strictness
   *   The strictness mode
   * @return
   *   Appropriate validation policy
   *
   * @example
   *   {{{
   * val policy = ValidationPolicy.fromStrictness(Strictness.Strict)
   * policy.validateArrayCount(3, 5, "items") // throws
   *   }}}
   */
  def fromStrictness(strictness: Strictness): ValidationPolicy = strictness match {
  case Strictness.Strict  => StrictValidationPolicy
  case Strictness.Lenient => LenientValidationPolicy
  }

}
