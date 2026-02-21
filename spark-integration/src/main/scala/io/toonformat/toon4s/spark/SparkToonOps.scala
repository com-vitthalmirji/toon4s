package io.toonformat.toon4s.spark

import scala.collection.immutable.VectorMap
import scala.jdk.CollectionConverters._
import scala.util.Try

import io.toonformat.toon4s.{DecodeOptions, EncodeOptions, Toon}
import io.toonformat.toon4s.JsonValue
import io.toonformat.toon4s.JsonValue._
import io.toonformat.toon4s.spark.error.SparkToonError
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructType

/**
 * Extension methods for DataFrame ↔ TOON conversion.
 *
 * ==Design principles==
 *   - Pure functional API with Either for error handling
 *   - Extension methods via implicit class (Scala idiom)
 *   - Chunking strategy for large datasets
 *   - Monadic composition with for-comprehensions
 *
 * ==Usage==
 * {{{
 * import io.toonformat.toon4s.spark.SparkToonOps._
 *
 * // Encode DataFrame to TOON
 * val result: Either[SparkToonError, Vector[String]] =
 *   df.toToon(key = "users", maxRowsPerChunk = 500)
 *
 * // Get token metrics
 * val metrics: Either[SparkToonError, ToonMetrics] =
 *   df.toonMetrics(key = "users")
 *
 * // Pattern matching for error handling
 * result match {
 *   case Right(toonStrings) => processSuccess(toonStrings)
 *   case Left(error) => handleError(error)
 * }
 *
 * // For-comprehension for chaining
 * val pipeline = for {
 *   toon <- df.toToon()
 *   metrics <- df.toonMetrics()
 * } yield (toon, metrics)
 * }}}
 */
object SparkToonOps {

  /**
   * Extension methods for DataFrame.
   *
   * Implicit class provides fluent API for TOON operations. Extends AnyVal for zero runtime
   * overhead.
   */
  implicit class ToonDataFrameOps(val df: DataFrame) extends AnyVal {

    /** Encode DataFrame to TOON using stable options model. */
    def toToon(
        options: ToonSparkOptions
    ): Either[SparkToonError, Vector[String]] = {
      encodeToChunksStreaming(
        df,
        options.key,
        options.maxRowsPerChunk,
        options.encodeOptions,
      )
    }

    /**
     * Encode DataFrame to TOON format with chunking.
     *
     * Pure function with explicit error channel (Either). Supports chunking for large DataFrames to
     * avoid driver memory pressure.
     *
     * @param key
     *   Top-level key for TOON document (e.g., "users", "events")
     * @param maxRowsPerChunk
     *   Maximum rows per TOON chunk (default 1000)
     * @param options
     *   TOON encoding options (delimiter, indent, etc.)
     * @return
     *   Either error or Vector of TOON strings (one per chunk)
     *
     * @example
     *   {{{
     * df.toToon(key = "orders", maxRowsPerChunk = 500) match {
     *   case Right(chunks) =>
     *     chunks.zipWithIndex.foreach { case (toon, i) =>
     *       println(s"Chunk \$i: \${toon.take(100)}...")
     *     }
     *   case Left(error) =>
     *     logger.error(s"Encoding failed: \${error.message}")
     * }
     *   }}}
     */
    def toToon(
        key: String = "data",
        maxRowsPerChunk: Int = 1000,
        options: EncodeOptions = EncodeOptions(),
    ): Either[SparkToonError, Vector[String]] = {
      toToon(ToonSparkOptions(key, maxRowsPerChunk, options))
    }

    /**
     * Compute token metrics comparing JSON vs TOON.
     *
     * Measures token count for both JSON and TOON encodings, computing savings percentage. Useful
     * for cost analysis and optimization decisions.
     *
     * @param key
     *   Top-level key for TOON document
     * @param options
     *   TOON encoding options
     * @return
     *   Either error or ToonMetrics with token counts
     *
     * @example
     *   {{{
     * df.toonMetrics() match {
     *   case Right(metrics) =>
     *     println(metrics.summary)
     *     if (metrics.hasMeaningfulSavings()) {
     *       println("TOON provides significant savings!")
     *     }
     *   case Left(error) =>
     *     logger.error(s"Metrics computation failed: \${error.message}")
     * }
     *   }}}
     */
    def toonMetrics(
        key: String = "data",
        options: EncodeOptions = EncodeOptions(),
    ): Either[SparkToonError, ToonMetrics] = {
      toonMetrics(key, maxRowsPerChunk = 1000, options = options)
    }

    /** Compute token metrics using stable options model. */
    def toonMetrics(
        options: ToonSparkOptions
    ): Either[SparkToonError, ToonMetrics] = {
      calculateMetricsStreaming(
        df,
        options.key,
        options.maxRowsPerChunk,
        options.encodeOptions,
      )
    }

    /**
     * Compute token metrics comparing JSON vs TOON using caller-provided chunk size.
     *
     * Keeps metric chunk boundaries aligned with production encoding behavior.
     */
    def toonMetrics(
        key: String,
        maxRowsPerChunk: Int,
        options: EncodeOptions,
    ): Either[SparkToonError, ToonMetrics] = {
      toonMetrics(ToonSparkOptions(key, maxRowsPerChunk, options))
    }

    /**
     * Show TOON sample (convenience method for debugging).
     *
     * Encodes a sample of the DataFrame and prints the TOON output. Useful for quick inspection.
     *
     * @param n
     *   Number of rows to sample (default 5)
     */
    def showToonSample(n: Int = 5): Unit = {
      df.limit(n).toToon(maxRowsPerChunk = n).fold(
        error => println(s"Error: ${error.message}"),
        toon =>
          toon.headOption.foreach { t =>
            println(s"TOON Sample ($n rows):")
            println(t)
          },
      )
    }

  }

  /**
   * Decode TOON documents back to DataFrame.
   *
   * Reconstructs DataFrame from TOON-encoded strings. Requires schema to validate and type-check
   * decoded data.
   *
   * @param toonDocuments
   *   Vector of TOON-encoded strings (e.g., from chunked encoding)
   * @param schema
   *   Expected DataFrame schema
   * @param options
   *   TOON decoding options
   * @param spark
   *   Implicit SparkSession for DataFrame creation
   * @return
   *   Either error or reconstructed DataFrame
   *
   * @example
   *   {{{
   * implicit val spark: SparkSession = SparkSession.builder().getOrCreate()
   *
   * val toonStrings: Vector[String] = loadFromStorage()
   * val schema = StructType(Seq(
   *   StructField("id", IntegerType),
   *   StructField("name", StringType)
   * ))
   *
   * SparkToonOps.fromToon(toonStrings, schema) match {
   *   case Right(df) => df.show()
   *   case Left(error) => logger.error(s"Decoding failed: \${error.message}")
   * }
   *   }}}
   */
  def fromToon(
      toonDocuments: Vector[String],
      schema: StructType,
      options: DecodeOptions = DecodeOptions(),
  )(implicit spark: SparkSession): Either[SparkToonError, DataFrame] = {

    // Step 1: Decode all TOON documents
    val decodedResults = toonDocuments.map(toon => decodeSafe(toon, options))

    // Step 2: Sequence Either values (short-circuit on first error)
    sequence(decodedResults).flatMap {
      jsonValues =>
        // Step 3: Extract rows from decoded JSON (robust to nested wrappers)
        val rows = extractRows(jsonValues)

      // Step 4: Convert to Spark Rows
      convertToSparkRows(rows, schema).map { sparkRows =>
        // Step 5: Create DataFrame without RDD APIs (Spark Connect compatible)
        spark.createDataFrame(sparkRows.asJava, schema)
      }
    }
  }

  // ========== Private Helper Functions ==========

  /**
   * Stream DataFrame rows and encode incrementally into TOON chunks.
   *
   * Uses `toLocalIterator` to avoid materializing the full dataset in driver memory.
   */
  private def encodeToChunksStreaming(
      df: DataFrame,
      key: String,
      maxRowsPerChunk: Int,
      options: EncodeOptions,
  ): Either[SparkToonError, Vector[String]] = {

    if (maxRowsPerChunk <= 0) {
      Left(SparkToonError.ConversionError("maxRowsPerChunk must be greater than 0"))
    } else {
      val schema = df.schema
      val iteratorResult = Try(df.toLocalIterator().asScala).toEither.left.map { ex =>
        SparkToonError.CollectionError(
          s"Failed to iterate DataFrame rows: ${ex.getMessage}",
          Some(ex),
        )
      }

      iteratorResult.flatMap { rowIterator =>
        val chunkRows = scala.collection.mutable.ArrayBuffer.empty[JsonValue]
        val encodedChunks = Vector.newBuilder[String]
        var sawAnyRow = false
        var maybeError: Option[SparkToonError] = None

        def flushChunkIfNeeded(force: Boolean): Unit = {
          val shouldFlush = chunkRows.nonEmpty && (force || chunkRows.size >= maxRowsPerChunk)
          if (maybeError.isEmpty && shouldFlush) {
            val wrappedChunk = JObj(VectorMap(key -> JArray(chunkRows.toVector)))
            encodeSafe(wrappedChunk, options) match {
            case Right(encoded) =>
              encodedChunks += encoded
              chunkRows.clear()
            case Left(err) =>
              maybeError = Some(err)
            }
          }
        }

        var continue = true
        while (continue && maybeError.isEmpty) {
          nextRowSafe(rowIterator) match {
          case Right(Some(row)) =>
            sawAnyRow = true
            SparkJsonInterop.rowToJsonValueSafe(row, schema) match {
            case Right(jsonRow) =>
              chunkRows += jsonRow
              flushChunkIfNeeded(force = false)
            case Left(err) =>
              maybeError = Some(err)
            }
          case Right(None) =>
            continue = false
          case Left(err) =>
            maybeError = Some(err)
          }
        }

        maybeError match {
        case Some(err) =>
          Left(err)
        case None =>
          if (!sawAnyRow) {
            val wrappedChunk = JObj(VectorMap(key -> JArray(Vector.empty)))
            encodeSafe(wrappedChunk, options).map(encoded => Vector(encoded))
          } else {
            flushChunkIfNeeded(force = true)
            maybeError match {
            case Some(err) => Left(err)
            case None      => Right(encodedChunks.result())
            }
          }
        }
      }
    }
  }

  /**
   * Stream DataFrame rows and compute JSON/TOON metrics incrementally.
   *
   * Uses chunk-local encoding and aggregates counts to avoid building a single huge payload.
   */
  private def calculateMetricsStreaming(
      df: DataFrame,
      key: String,
      maxRowsPerChunk: Int,
      options: EncodeOptions,
  ): Either[SparkToonError, ToonMetrics] = {
    if (maxRowsPerChunk <= 0) {
      Left(SparkToonError.ConversionError("maxRowsPerChunk must be greater than 0"))
    } else {
      val chunkSize = maxRowsPerChunk
      val schema = df.schema

      val iteratorResult = Try(df.toLocalIterator().asScala).toEither.left.map { ex =>
        SparkToonError.CollectionError(
          s"Failed to iterate DataFrame rows: ${ex.getMessage}",
          Some(ex),
        )
      }

      iteratorResult.flatMap { rowIterator =>
        val chunkRows = scala.collection.mutable.ArrayBuffer.empty[JsonValue]
        var sawAnyRow = false
        var maybeError: Option[SparkToonError] = None
        var jsonTokenCount = 0
        var toonTokenCount = 0
        var rowCount = 0

        def flushMetricsChunkIfNeeded(force: Boolean): Unit = {
          val shouldFlush = chunkRows.nonEmpty && (force || chunkRows.size >= chunkSize)
          if (maybeError.isEmpty && shouldFlush) {
            val wrappedChunk = JObj(VectorMap(key -> JArray(chunkRows.toVector)))
            val jsonBaseline = encodeAsJson(wrappedChunk)
            encodeSafe(wrappedChunk, options) match {
            case Right(toonEncoded) =>
              val chunkMetrics = ToonMetrics.fromEncodedStrings(
                jsonEncoded = jsonBaseline,
                toonEncoded = toonEncoded,
                rowCount = chunkRows.size,
                columnCount = schema.fields.length,
              )
              jsonTokenCount += chunkMetrics.jsonTokenCount
              toonTokenCount += chunkMetrics.toonTokenCount
              rowCount += chunkRows.size
              chunkRows.clear()
            case Left(err) =>
              maybeError = Some(err)
            }
          }
        }

        var continue = true
        while (continue && maybeError.isEmpty) {
          nextRowSafe(rowIterator) match {
          case Right(Some(row)) =>
            sawAnyRow = true
            SparkJsonInterop.rowToJsonValueSafe(row, schema) match {
            case Right(jsonRow) =>
              chunkRows += jsonRow
              flushMetricsChunkIfNeeded(force = false)
            case Left(err) =>
              maybeError = Some(err)
            }
          case Right(None) =>
            continue = false
          case Left(err) =>
            maybeError = Some(err)
          }
        }

        maybeError match {
        case Some(err) =>
          Left(err)
        case None =>
          if (!sawAnyRow) {
            val wrappedChunk = JObj(VectorMap(key -> JArray(Vector.empty)))
            val jsonBaseline = encodeAsJson(wrappedChunk)
            encodeSafe(wrappedChunk, options).map { toonEncoded =>
              ToonMetrics.fromEncodedStrings(
                jsonEncoded = jsonBaseline,
                toonEncoded = toonEncoded,
                rowCount = 0,
                columnCount = schema.fields.length,
              )
            }
          } else {
            flushMetricsChunkIfNeeded(force = true)
            maybeError match {
            case Some(err) =>
              Left(err)
            case None =>
              Right(ToonMetrics(
                jsonTokenCount = jsonTokenCount,
                toonTokenCount = toonTokenCount,
                rowCount = rowCount,
                columnCount = schema.fields.length,
              ))
            }
          }
        }
      }
    }
  }

  /** Safely fetch next row from iterator, including `hasNext` failures. */
  private def nextRowSafe(
      rowIterator: Iterator[Row]
  ): Either[SparkToonError, Option[Row]] = {
    Try {
      if (rowIterator.hasNext) Some(rowIterator.next())
      else None
    }.toEither.left.map { ex =>
      SparkToonError.CollectionError(
        s"Failed while reading DataFrame rows: ${ex.getMessage}",
        Some(ex),
      )
    }
  }

  /** Convert Spark Rows to JsonValue array. */
  private def convertRowsToJsonArray(
      rows: Array[Row],
      schema: StructType,
  ): Either[SparkToonError, JArray] = {

    // Convert each row, short-circuiting on first error
    val jsonResults = rows.map(row => SparkJsonInterop.rowToJsonValueSafe(row, schema))

    sequence(jsonResults.toVector).map(JArray.apply)
  }

  /** Convert JsonValue rows to Spark Rows. */
  private def convertToSparkRows(
      rows: Vector[JsonValue],
      schema: StructType,
  ): Either[SparkToonError, Vector[Row]] = {

    val rowResults = rows.map(json => SparkJsonInterop.jsonValueToRowSafe(json, schema))

    sequence(rowResults)
  }

  /** Encode JsonValue array in chunks. */
  private def encodeChunks(
      jsonArray: JArray,
      key: String,
      maxRowsPerChunk: Int,
      options: EncodeOptions,
  ): Either[SparkToonError, Vector[String]] = {

    val rawChunks = jsonArray.value.grouped(maxRowsPerChunk).toVector
    // Ensure we always emit at least one chunk, even for empty inputs
    val chunks =
      if (rawChunks.isEmpty) Vector(Vector.empty[JsonValue])
      else rawChunks

    // Encode each chunk, short-circuiting on first error
    val chunkResults = chunks.map { chunk =>
      val wrappedChunk = JObj(VectorMap(key -> JArray(chunk)))
      encodeSafe(wrappedChunk, options)
    }

    sequence(chunkResults)
  }

  /** Safely encode JsonValue to TOON. */
  private def encodeSafe(
      json: JsonValue,
      options: EncodeOptions,
  ): Either[SparkToonError, String] = {
    Toon.encode(json, options).left.map(err => SparkToonError.EncodingError(err))
  }

  /** Safely decode TOON string to JsonValue. */
  private def decodeSafe(
      toon: String,
      options: DecodeOptions,
  ): Either[SparkToonError, JsonValue] = {
    Toon.decode(toon, options).left.map(err => SparkToonError.DecodingError(err))
  }

  /**
   * Extract rows from decoded JsonValues.
   *
   * Handles both wrapped (JObj with key) and unwrapped (direct JArray) formats.
   */
  private def extractRows(jsonValues: Vector[JsonValue]): Vector[JsonValue] = {
    def findRowArray(value: JsonValue): Option[Vector[JsonValue]] = value match {
    // Direct array of row objects
    case JArray(rows) if rows.forall {
          case JObj(_) => true
          case _       => false
        } =>
      Some(rows)

    // Object wrapper – search values recursively
    case JObj(fields) =>
      fields.valuesIterator
        .map(findRowArray)
        .collectFirst { case Some(rows) => rows }

    // Other JSON shapes are ignored
    case _ => None
    }

    jsonValues.flatMap(v => findRowArray(v).getOrElse(Vector.empty))
  }

  /** Encode a JsonValue as a minimal JSON string for baseline token estimates. */
  private def encodeAsJson(value: JsonValue): String = {
    def escapeString(s: String): String = {
      val sb = new StringBuilder(s.length + 16)
      s.foreach {
        case '"'              => sb.append("\\\"")
        case '\\'             => sb.append("\\\\")
        case '\b'             => sb.append("\\b")
        case '\f'             => sb.append("\\f")
        case '\n'             => sb.append("\\n")
        case '\r'             => sb.append("\\r")
        case '\t'             => sb.append("\\t")
        case c if c.isControl =>
          sb.append(f"\\u${c.toInt}%04x")
        case c =>
          sb.append(c)
      }
      sb.result()
    }

    def encode(value: JsonValue): String = value match {
    case JsonValue.JNull          => "null"
    case JsonValue.JBool(b)       => if (b) "true" else "false"
    case JsonValue.JNumber(n)     => n.bigDecimal.stripTrailingZeros.toPlainString
    case JsonValue.JString(s)     => "\"" + escapeString(s) + "\""
    case JsonValue.JArray(values) =>
      values.iterator.map(encode).mkString("[", ",", "]")
    case JsonValue.JObj(fields) =>
      fields
        .iterator
        .map { case (k, v) => "\"" + escapeString(k) + "\":" + encode(v) }
        .mkString("{", ",", "}")
    }

    encode(value)
  }

  /**
   * Sequence Vector of Either values.
   *
   * Monadic traversal that short-circuits on first Left. Equivalent to Cats' sequence but
   * implemented manually to avoid dependency.
   *
   * @param eithers
   *   Vector of Either values
   * @return
   *   Either of first error or Vector of successes
   */
  private def sequence[E, A](eithers: Vector[Either[E, A]]): Either[E, Vector[A]] = {
    eithers.foldLeft[Either[E, Vector[A]]](Right(Vector.empty)) {
      case (Right(acc), Right(value)) => Right(acc :+ value)
      case (Left(err), _)             => Left(err)
      case (_, Left(err))             => Left(err)
    }
  }

}
