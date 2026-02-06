package io.toonformat.toon4s.spark

import scala.util.Try

import io.toonformat.toon4s.{DecodeOptions, EncodeOptions, Toon}
import io.toonformat.toon4s.JsonValue._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

/**
 * User-defined Functions (UDFs) for TOON encoding/decoding in Spark SQL.
 *
 * ==Design==
 * Provides SQL functions that can be used in Spark SQL queries for TOON operations.
 *
 * NOTE: row-level UDFs operate on string representations and are best for ad-hoc SQL checks. For
 * canonical DataFrame encoding/decoding, use `SparkToonOps.toToon` and `SparkToonOps.fromToon`.
 *
 * ==Usage==
 * {{{
 * import io.toonformat.toon4s.spark.ToonUDFs
 *
 * // Register all UDFs with SparkSession
 * ToonUDFs.register(spark)
 *
 * // Use in SQL
 * spark.sql("""
 *   SELECT
 *     id,
 *     name,
 *     toon_encode_row(struct(id, name, email)) as toon_payload
 *   FROM users
 *   WHERE id < 1000
 * """)
 *
 * // Use in DataFrame API
 * import org.apache.spark.sql.functions._
 * df.withColumn("toon", expr("toon_encode_row(struct(*))"))
 * }}}
 */
object ToonUDFs {

  /**
   * Register all TOON UDFs with SparkSession.
   *
   * Makes UDFs available in both Scala DataFrame API and Spark SQL queries.
   *
   * @param spark
   *   SparkSession to register UDFs with
   * @param prefix
   *   Optional prefix for function names (default: "toon_")
   *
   * @example
   *   {{{
   * val spark = SparkSession.builder().getOrCreate()
   * ToonUDFs.register(spark)
   *
   * // Now you can use toon_encode_row, toon_decode_row, etc. in SQL
   *   }}}
   */
  def register(spark: SparkSession, prefix: String = "toon_"): Unit = {
    spark.udf.register(s"${prefix}encode_row", encodeRowUDF)
    spark.udf.register(s"${prefix}decode_row", decodeRowUDF)
    spark.udf.register(s"${prefix}encode_string", encodeStringUDF)
    spark.udf.register(s"${prefix}decode_string", decodeStringUDF)
    spark.udf.register(s"${prefix}estimate_tokens", estimateTokensUDF)
  }

  /**
   * Return the Spark row text representation.
   *
   * This function does not create canonical TOON output for structs. It keeps row text as-is and is
   * useful for SQL pipelines that only need a simple row representation.
   *
   * @example
   *   {{{
   * import org.apache.spark.sql.functions._
   *
   * df.withColumn("toon_encoded",
   *   ToonUDFs.encodeRowUDF(struct("id", "name", "email"))
   * )
   *   }}}
   */
  val encodeRowUDF: UserDefinedFunction = udf { (row: String) =>
    // Note: UDFs receive string representation of struct, not actual Row object
    // This is a limitation of Spark SQL UDF serialization
    Try {
      // Keep row text as-is. Use SparkToonOps.toToon for canonical row encoding.
      row
    }.getOrElse(s"ERROR: Failed to encode row")
  }

  /**
   * Decode TOON string to text representation.
   *
   * Returns JSON-like text on success and null on failure. For typed schema-aware decoding, use
   * SparkToonOps.fromToon.
   *
   * @example
   *   {{{
   * df.withColumn("decoded",
   *   ToonUDFs.decodeRowUDF(col("toon_payload"))
   * )
   *   }}}
   */
  val decodeRowUDF: UserDefinedFunction = udf { (toon: String) =>
    Toon.decode(toon, DecodeOptions()).fold(
      _ => null, // Return null on error (SQL-friendly)
      json => {
        // Convert JsonValue to string representation
        // Users should use fromToon for DataFrame-level decoding
        json.toString
      },
    )
  }

  /**
   * Encode any string value to TOON format.
   *
   * Simple wrapper around Toon.encode for string values. Useful for encoding individual fields.
   *
   * @example
   *   {{{
   * df.withColumn("toon_name",
   *   ToonUDFs.encodeStringUDF(col("name"))
   * )
   *   }}}
   */
  val encodeStringUDF: UserDefinedFunction = udf { (value: String) =>
    if (value == null) "null"
    else {
      Toon.encode(JString(value), EncodeOptions()).getOrElse(s"\"$value\"")
    }
  }

  /**
   * Decode TOON string to plain string.
   *
   * @example
   *   {{{
   * df.withColumn("decoded_name",
   *   ToonUDFs.decodeStringUDF(col("toon_name"))
   * )
   *   }}}
   */
  val decodeStringUDF: UserDefinedFunction = udf { (toon: String) =>
    Toon.decode(toon, DecodeOptions()).fold(
      _ => null,
      json => {
        json match {
        case JString(s) => s
        case _          => json.toString
        }
      },
    )
  }

  /**
   * Estimate token count for a string.
   *
   * Uses the same estimation algorithm as ToonMetrics (4 characters per token).
   *
   * @example
   *   {{{
   * df.withColumn("token_count",
   *   ToonUDFs.estimateTokensUDF(col("toon_payload"))
   * )
   *   }}}
   */
  val estimateTokensUDF: UserDefinedFunction = udf { (text: String) =>
    if (text == null || text.isEmpty) 0
    else ToonMetrics.estimateTokens(text)
  }

}

/**
 * Advanced UDFs for batch operations.
 *
 * These UDFs operate on arrays/collections for better performance in batch scenarios.
 */
object ToonBatchUDFs {

  /**
   * Encode array of strings to TOON array format.
   *
   * @example
   *   {{{
   * df.withColumn("toon_array",
   *   ToonBatchUDFs.encodeArrayUDF(col("tags"))
   * )
   *   }}}
   */
  val encodeArrayUDF: UserDefinedFunction = udf { (values: Seq[String]) =>
    if (values == null) "[]"
    else {
      val jsonArray = JArray(values.map(JString.apply).toVector)
      Toon.encode(jsonArray, EncodeOptions()).getOrElse("[]")
    }
  }

  /** Decode TOON array to Seq of strings. */
  val decodeArrayUDF: UserDefinedFunction = udf { (toon: String) =>
    Toon.decode(toon, DecodeOptions()).fold(
      _ => Seq.empty[String],
      json => {
        json match {
        case JArray(values) =>
          values.collect { case JString(s) => s }
        case _ => Seq.empty[String]
        }
      },
    )
  }

  /** Register batch UDFs. */
  def register(spark: SparkSession, prefix: String = "toon_"): Unit = {
    spark.udf.register(s"${prefix}encode_array", encodeArrayUDF)
    spark.udf.register(s"${prefix}decode_array", decodeArrayUDF)
  }

}
