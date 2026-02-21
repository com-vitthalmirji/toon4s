package io.toonformat.toon4s.spark.error

import io.toonformat.toon4s.error.{DecodeError, EncodeError}

/**
 * Root error type for Spark integration failures.
 *
 * Sealed trait ensures exhaustive pattern matching at compile time. All subtypes are defined in the
 * same file for compiler safety and to prevent missing cases in pattern matches.
 *
 * ==Design principles==
 *   - Algebraic Data Type (ADT) for type-safe error modeling
 *   - Immutable case classes for error instances
 *   - Optional cause for error chaining
 *   - Message abstraction for consistent error reporting
 *
 * @example
 *   {{{
 * import io.toonformat.toon4s.spark.error.SparkToonError
 *
 * df.toToon() match {
 *   case Right(toon) => processSuccess(toon)
 *   case Left(SparkToonError.ConversionError(msg, cause)) =>
 *     logger.error(s"Conversion failed: \$msg", cause.orNull)
 *   case Left(SparkToonError.EncodingError(toonErr, _)) =>
 *     logger.error(s"TOON encoding failed: \$toonErr")
 *   case Left(error) =>
 *     logger.error(s"Error: \${error.message}")
 * }
 *   }}}
 */
sealed trait SparkToonError {

  /** Human-readable error message */
  def message: String

  /** Optional underlying cause (for error chaining) */
  def cause: Option[Throwable]

}

object SparkToonError {

  /**
   * Spark Row to JsonValue conversion failed.
   *
   * This error occurs when translating Spark DataTypes to toon4s JsonValue types. Common causes:
   *   - Unsupported Spark data types
   *   - Null pointer exceptions in nested structures
   *   - Schema mismatches during conversion
   *
   * @param message
   *   Description of what went wrong
   * @param cause
   *   Optional underlying exception
   */
  final case class ConversionError(message: String, cause: Option[Throwable] = None)
      extends SparkToonError

  /**
   * TOON encoding failed.
   *
   * Wraps errors from toon4s-core's Toon.encode method. The underlying EncodeError provides
   * detailed information about why encoding failed.
   *
   * @param toonError
   *   The underlying EncodeError from toon4s-core
   * @param cause
   *   Optional underlying exception
   */
  final case class EncodingError(toonError: EncodeError, cause: Option[Throwable] = None)
      extends SparkToonError {

    def message: String = s"TOON encoding failed: ${toonError.getMessage}"

  }

  /**
   * TOON decoding failed.
   *
   * Wraps errors from toon4s-core's Toon.decode method. The underlying DecodeError provides
   * detailed information about parsing failures.
   *
   * @param toonError
   *   The underlying DecodeError from toon4s-core
   * @param cause
   *   Optional underlying exception
   */
  final case class DecodingError(toonError: DecodeError, cause: Option[Throwable] = None)
      extends SparkToonError {

    def message: String = s"TOON decoding failed: ${toonError.getMessage}"

  }

  /**
   * Schema mismatch between DataFrame and JsonValue.
   *
   * This error occurs when the expected schema does not match the actual data structure. Common
   * scenarios:
   *   - Missing required columns
   *   - Type mismatches (expected Int, got String)
   *   - Extra columns in decoded data
   *
   * @param expected
   *   String representation of expected schema
   * @param actual
   *   String representation of actual schema
   * @param cause
   *   Optional underlying exception
   */
  final case class SchemaMismatch(
      expected: String,
      actual: String,
      cause: Option[Throwable] = None,
  ) extends SparkToonError {

    def message: String = s"Schema mismatch: expected=$expected, actual=$actual"

  }

  /**
   * Materialization failed (driver out of memory, dataset too large).
   *
   * This error occurs when driver-side extraction fails due to memory pressure. Solutions:
   *   - Reduce maxRowsPerChunk parameter
   *   - Filter DataFrame before encoding
   *   - Use chunked processing with toToonChunked
   *
   * @param message
   *   Description of collection failure
   * @param cause
   *   Optional underlying exception
   */
  final case class CollectionError(message: String, cause: Option[Throwable] = None)
      extends SparkToonError

  /**
   * Unsupported Spark data type.
   *
   * This error occurs when a Spark DataType cannot be mapped to JsonValue. Currently unsupported
   * types:
   *   - CalendarIntervalType
   *   - UserDefinedType (UDT) without custom converters
   *
   * @param dataType
   *   String representation of unsupported type
   * @param cause
   *   Optional underlying exception
   */
  final case class UnsupportedDataType(dataType: String, cause: Option[Throwable] = None)
      extends SparkToonError {

    def message: String = s"Unsupported Spark DataType: $dataType"

  }

}
