package io.toonformat.toon4s
package decode

import io.toonformat.toon4s.{Constants => C, DecodeOptions, Delimiter, Strictness}
import io.toonformat.toon4s.decode.context.ParseContext
import io.toonformat.toon4s.decode.parsers.ArrayHeaderInfo
import io.toonformat.toon4s.decode.validation.ValidationPolicy
import io.toonformat.toon4s.error.ErrorLocation

/**
 * Validation utilities for TOON decoding.
 *
 * ==Design: Adapter + Policy pattern==
 *
 * This object adapts the old validation API to the new [[validation.ValidationPolicy]] pattern,
 * maintaining backward compatibility while enabling testable policies.
 *
 * ==Migration plan==
 * Existing code uses implicit `Strictness` parameters. This object converts those to explicit
 * `ValidationPolicy` instances internally.
 */
private[decode] object Validation {

  /**
   * Validate depth limit to prevent stack overflow attacks.
   *
   * Always enforced (security), regardless of strictness.
   *
   * @param currentDepth
   *   Current nesting level
   * @param options
   *   Decode options containing depth limit
   */
  def validateDepth(currentDepth: Int, options: DecodeOptions): Unit = {
    val policy = ValidationPolicy.fromStrictness(options.strictness)
    policy.validateDepth(currentDepth, options.maxDepth)
  }

  /**
   * Validate depth limit with error location context.
   *
   * ==Enhanced error tracking==
   * This version enriches errors with ParseContext location information.
   *
   * @param currentDepth
   *   Current nesting level
   * @param options
   *   Decode options containing depth limit
   * @param ctx
   *   Parse context for error location
   */
  def validateDepth(currentDepth: Int, options: DecodeOptions, ctx: ParseContext): Unit = {
    ctx.withLocation {
      validateDepth(currentDepth, options)
    }
  }

  /**
   * Validate array length limit to prevent memory exhaustion.
   *
   * Always enforced (security), regardless of strictness.
   *
   * @param length
   *   Array length
   * @param options
   *   Decode options containing length limit
   */
  def validateArrayLength(length: Int, options: DecodeOptions): Unit = {
    val policy = ValidationPolicy.fromStrictness(options.strictness)
    policy.validateArrayLength(length, options.maxArrayLength)
  }

  /**
   * Validate array length limit with error location context.
   *
   * @param length
   *   Array length
   * @param options
   *   Decode options containing length limit
   * @param ctx
   *   Parse context for error location
   */
  def validateArrayLength(length: Int, options: DecodeOptions, ctx: ParseContext): Unit = {
    ctx.withLocation {
      validateArrayLength(length, options)
    }
  }

  /**
   * Validate string length limit to prevent memory exhaustion.
   *
   * Always enforced (security), regardless of strictness.
   *
   * @param length
   *   String length
   * @param options
   *   Decode options containing length limit
   */
  def validateStringLength(length: Int, options: DecodeOptions): Unit = {
    val policy = ValidationPolicy.fromStrictness(options.strictness)
    policy.validateStringLength(length, options.maxStringLength)
  }

  /**
   * Validate string length limit with error location context.
   *
   * @param length
   *   String length
   * @param options
   *   Decode options containing length limit
   * @param ctx
   *   Parse context for error location
   */
  def validateStringLength(length: Int, options: DecodeOptions, ctx: ParseContext): Unit = {
    ctx.withLocation {
      validateStringLength(length, options)
    }
  }

  /**
   * Assert that actual count matches expected count.
   *
   * Behavior depends on strictness:
   *   - Strict: Throws error on mismatch
   *   - Lenient: Accepts silently
   *
   * @param actual
   *   Actual count
   * @param expected
   *   Expected count
   * @param itemType
   *   Description of items
   * @param strictness
   *   Strictness mode
   */
  def assertExpectedCount(
      actual: Int,
      expected: Int,
      itemType: String,
  )(implicit strictness: Strictness): Unit = {
    val policy = ValidationPolicy.fromStrictness(strictness)
    policy.validateArrayCount(actual, expected, itemType)
  }

  /**
   * Validate that no extra list items exist beyond expected count.
   *
   * @param cursor
   *   Line cursor to check
   * @param itemDepth
   *   Depth of list items
   * @param expectedCount
   *   Expected number of items
   * @param strictness
   *   Strictness mode
   */
  def validateNoExtraListItems(
      cursor: LineCursor,
      itemDepth: Int,
      expectedCount: Int,
  )(implicit strictness: Strictness): Unit = {
    val hasExtra = !cursor.atEnd && cursor.peek.exists {
      next =>
        next.depth == itemDepth && (next.content.startsWith(
          C.ListItemPrefix
        ) || next.content == C.ListItemMarker)
    }

    val policy = ValidationPolicy.fromStrictness(strictness)
    policy.validateNoExtraItems(hasExtra, expectedCount, "list array items")
  }

  /**
   * Validate that no extra tabular rows exist beyond expected count.
   *
   * @param cursor
   *   Line cursor to check
   * @param rowDepth
   *   Depth of table rows
   * @param header
   *   Array header info
   * @param strictness
   *   Strictness mode
   */
  def validateNoExtraTabularRows(
      cursor: LineCursor,
      rowDepth: Int,
      header: ArrayHeaderInfo,
  )(implicit strictness: Strictness): Unit = {
    val hasExtra = !cursor.atEnd && cursor.peek.exists {
      next =>
        next.depth == rowDepth &&
        !next.content.startsWith(C.ListItemPrefix) &&
        isDataRow(next.content, header.delimiter)
    }

    val policy = ValidationPolicy.fromStrictness(strictness)
    policy.validateNoExtraItems(hasExtra, header.length, "tabular rows")
  }

  /**
   * Validate that no blank lines exist in a range.
   *
   * @param startLine
   *   Start of range (exclusive)
   * @param endLine
   *   End of range (exclusive)
   * @param blankLines
   *   Vector of blank line positions
   * @param context
   *   Description of context (e.g., "list array")
   * @param strictness
   *   Strictness mode
   */
  def validateNoBlankLinesInRange(
      startLine: Int,
      endLine: Int,
      blankLines: Vector[BlankLine],
      context: String,
  )(implicit strictness: Strictness): Unit = {
    val hasBlankLines = blankLines.exists {
      blank => blank.lineNumber > startLine && blank.lineNumber < endLine
    }

    val policy = ValidationPolicy.fromStrictness(strictness)
    policy.validateNoBlankLines(hasBlankLines, context)
  }

  /**
   * Check if content represents a data row in tabular format.
   *
   * ==Pure Predicate Function==
   *
   * A data row has delimiter before colon (or no colon at all).
   *
   * @param content
   *   Line content
   * @param delimiter
   *   Delimiter character
   * @return
   *   true if content is a data row
   */
  private def isDataRow(content: String, delimiter: Delimiter): Boolean = {
    val colonPos = content.indexOf(C.Colon)
    val delimiterPos = content.indexOf(delimiter.char)
    if (colonPos == -1) true
    else if (delimiterPos != -1 && delimiterPos < colonPos) true
    else false
  }

}
