package io.toonformat.toon4s.spark

import scala.collection.immutable.VectorMap
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal

import io.toonformat.toon4s.{DecodeOptions, Delimiter, EncodeOptions, Toon}
import io.toonformat.toon4s.JsonValue
import io.toonformat.toon4s.JsonValue._
import io.toonformat.toon4s.encode.{Encoders => ToonEncoders, Primitives}
import io.toonformat.toon4s.spark.error.SparkToonError
import io.toonformat.toon4s.spark.internal.SparkConfUtils
import io.toonformat.toon4s.spark.llm.{
  LlmChunkRequest,
  LlmPartitionIdempotency,
  LlmPartitionWriteMetrics,
  LlmPartitionWriteOptions,
  LlmPartitionWriterFactory,
  LlmRetry,
  LlmSendStatus,
}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

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

  final private class SparkToonEncodingException(val error: SparkToonError)
      extends IllegalStateException(error.message) {

    error.cause.foreach(initCause)

  }

  private val MaxCollectedChunksConfKey = "toon4s.spark.collect.maxChunks"

  private val MaxCollectedPayloadBytesConfKey = "toon4s.spark.collect.maxPayloadBytes"

  private val MaxCollectedPartitionMetricsConfKey = "toon4s.spark.metrics.maxPartitionStats"

  private val DefaultMaxCollectedChunks = 10000

  private val DefaultMaxCollectedPayloadBytes = 64L * 1024L * 1024L

  private val DefaultMaxCollectedPartitionMetrics = 20000

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
      if (options.maxRowsPerChunk <= 0) {
        Left(SparkToonError.ConversionError("maxRowsPerChunk must be greater than 0"))
      } else {
        collectToonChunks(
          toToonDataset(options),
          options.key,
          options.encodeOptions,
        )
      }
    }

    /**
     * Distributed TOON encoding that runs at partition level and returns a Dataset of TOON chunks.
     *
     * This method is executor-side and avoids row-level encoding on the driver.
     */
    def toToonDataset(options: ToonSparkOptions): Dataset[String] = {
      encodeToChunkDataset(
        df,
        options.key,
        options.maxRowsPerChunk,
        options.encodeOptions,
      )
    }

    /**
     * Write TOON chunks directly to storage without collecting payloads on the driver.
     *
     * Use this for production pipelines and large datasets.
     */
    def writeToon(
        outputPath: String,
        options: ToonSparkOptions,
    ): Either[SparkToonError, Unit] = {
      Try {
        toToonDataset(options)
          .write
          .mode("overwrite")
          .text(outputPath)
      }.toEither.left.map { ex =>
        SparkToonError.ConversionError(
          s"Failed to write TOON chunks to path '$outputPath': ${ex.getMessage}",
          Some(ex),
        )
      }
    }

    /**
     * Encode TOON on executors and send each chunk from partition tasks.
     *
     * This keeps both encode and external send work in Spark task cores.
     */
    def writeToLlmPartitions(
        writerFactory: LlmPartitionWriterFactory,
        llmOptions: LlmPartitionWriteOptions = LlmPartitionWriteOptions(),
        options: ToonSparkOptions = ToonSparkOptions(),
    ): Either[SparkToonError, LlmPartitionWriteMetrics] = {
      if (options.maxRowsPerChunk <= 0) {
        Left(SparkToonError.ConversionError("maxRowsPerChunk must be greater than 0"))
      } else {
        writeChunksToLlmPartitions(
          chunks = toToonDataset(options),
          key = options.key,
          writerFactory = writerFactory,
          llmOptions = llmOptions,
        )
      }
    }

    /** Convenience overload for executor-side LLM sink. */
    def writeToLlmPartitions(
        writerFactory: LlmPartitionWriterFactory,
        key: String,
        maxRowsPerChunk: Int,
        options: EncodeOptions,
        llmOptions: LlmPartitionWriteOptions,
    ): Either[SparkToonError, LlmPartitionWriteMetrics] = {
      writeToLlmPartitions(
        writerFactory = writerFactory,
        llmOptions = llmOptions,
        options = ToonSparkOptions(key, maxRowsPerChunk, options),
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

    /** Write TOON chunks directly using convenience parameters. */
    def writeToon(
        outputPath: String,
        key: String = "data",
        maxRowsPerChunk: Int = 1000,
        options: EncodeOptions = EncodeOptions(),
    ): Either[SparkToonError, Unit] = {
      writeToon(
        outputPath = outputPath,
        options = ToonSparkOptions(key, maxRowsPerChunk, options),
      )
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
      toonMetrics(options, ToonMetrics.defaultTokenEstimator)
    }

    /** Compute token metrics using stable options model and custom estimator. */
    def toonMetrics(
        options: ToonSparkOptions,
        tokenEstimator: ToonMetrics.TokenEstimator,
    ): Either[SparkToonError, ToonMetrics] = {
      calculateMetricsDistributed(
        df,
        options.key,
        options.maxRowsPerChunk,
        options.encodeOptions,
        tokenEstimator,
      )
    }

    /** Compute token metrics using partition-level execution and compact aggregation. */
    def toonMetricsDistributed(
        options: ToonSparkOptions
    ): Either[SparkToonError, ToonMetrics] = {
      toonMetrics(options)
    }

    /** Compute token metrics with a caller-provided token estimator. */
    def toonMetricsWithEstimator(
        key: String = "data",
        options: EncodeOptions = EncodeOptions(),
        tokenEstimator: ToonMetrics.TokenEstimator,
    ): Either[SparkToonError, ToonMetrics] = {
      toonMetrics(
        ToonSparkOptions(key = key, maxRowsPerChunk = 1000, encodeOptions = options),
        tokenEstimator,
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
      toonMetrics(
        ToonSparkOptions(key, maxRowsPerChunk, options),
        ToonMetrics.defaultTokenEstimator,
      )
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
    val toonDataset = spark.createDataset(toonDocuments)(Encoders.STRING)
    fromToonDataset(toonDataset, schema, options)
  }

  /**
   * Decode TOON Dataset back to DataFrame using partition-level Spark execution.
   *
   * This path avoids driver-side row decoding and scales for large TOON payloads.
   */
  def fromToonDataset(
      toonDocuments: Dataset[String],
      schema: StructType,
      options: DecodeOptions = DecodeOptions(),
  ): Either[SparkToonError, DataFrame] = {
    val decodedJsonRows = toonDocuments.mapPartitions { chunks =>
      chunks.flatMap { chunk =>
        decodeSafe(chunk, options) match {
        case Right(value) =>
          extractRowsFromValue(value).iterator.map(encodeAsJson)
        case Left(err) =>
          throw new SparkToonEncodingException(err)
        }
      }
    }(Encoders.STRING)

    Try(toonDocuments.sparkSession.read.schema(schema).json(decodedJsonRows)).toEither.left.map {
      ex =>
        SparkToonError.ConversionError(
          s"Failed to decode TOON dataset to DataFrame: ${ex.getMessage}",
          Some(ex),
        )
    }
  }

  // ========== Private Helper Functions ==========

  final case class PartitionMetrics(
      jsonTokenCount: Int,
      toonTokenCount: Int,
      rowCount: Int,
      columnCount: Int,
      errorMessage: Option[String],
  )

  final case class LlmPartitionMetrics(
      attemptedChunks: Long,
      sentChunks: Long,
      duplicateChunks: Long,
      failedChunks: Long,
      totalBytes: Long,
      totalEstimatedTokens: Long,
      errorMessage: Option[String],
  )

  /**
   * Distributed partition-level encoding to TOON chunks.
   *
   * Uses Dataset.mapPartitions so conversion happens on executors.
   */
  private def encodeToChunkDataset(
      df: DataFrame,
      key: String,
      maxRowsPerChunk: Int,
      options: EncodeOptions,
  ): Dataset[String] = {
    require(maxRowsPerChunk > 0, "maxRowsPerChunk must be greater than 0")

    if (isFlatTabularSchema(df.schema)) {
      encodeFlatChunkDataset(df, key, maxRowsPerChunk, options)
    } else {
      encodeNestedChunkDataset(df, key, maxRowsPerChunk, options)
    }
  }

  /** A schema is flat-tabular when every top-level column is a primitive (no struct, array or map). */
  private def isFlatTabularSchema(schema: StructType): Boolean =
    schema.fields.nonEmpty && schema.fields.forall { field =>
      field.dataType match {
      case _: StructType | _: ArrayType | _: MapType => false
      case _                                         => true
      }
    }

  /**
   * Fast path for flat schemas: format each typed cell straight to a canonical TOON token and emit
   * the tabular block with the core writer. Avoids the per-row JsonValue, BigDecimal, VectorMap and
   * Try of the general path.
   */
  private def encodeFlatChunkDataset(
      df: DataFrame,
      key: String,
      maxRowsPerChunk: Int,
      options: EncodeOptions,
  ): Dataset[String] = {
    val schema = df.schema
    val fieldNames = schema.fields.map(_.name)
    val fieldTypes = schema.fields.map(_.dataType)
    val delim = options.delimiter

    df.mapPartitions { rows =>
      val buffer = new scala.collection.mutable.ArrayBuffer[Array[String]](maxRowsPerChunk)
      val chunks = scala.collection.mutable.ArrayBuffer.empty[String]

      def flush(force: Boolean): Unit = {
        if (buffer.nonEmpty && (force || buffer.size >= maxRowsPerChunk)) {
          chunks += ToonEncoders.encodeTabularChunk(key, fieldNames, buffer.toVector, options)
          buffer.clear()
        }
      }

      rows.foreach { row =>
        val tokens = new Array[String](fieldTypes.length)
        var i = 0
        while (i < fieldTypes.length) {
          tokens(i) = formatCell(row, i, fieldTypes(i), delim)
          i += 1
        }
        buffer += tokens
        flush(force = false)
      }

      flush(force = true)
      chunks.iterator
    }(Encoders.STRING)
  }

  /**
   * Format a single typed cell to a canonical TOON token. Mirrors
   * `SparkJsonInterop.fieldToJsonValue` followed by `encodePrimitive` so the output is identical to
   * the general encode path.
   */
  private def formatCell(row: Row, i: Int, dataType: DataType, delim: Delimiter): String =
    if (row.isNullAt(i)) Primitives.nullToken
    else
      dataType match {
      case StringType  => Primitives.encodeStringLiteral(row.getString(i), delim)
      case IntegerType => Primitives.formatLong(row.getInt(i).toLong)
      case LongType    => Primitives.formatLong(row.getLong(i))
      case DoubleType  =>
        val d = row.getDouble(i)
        if (d.isNaN || d.isInfinity) Primitives.nullToken else Primitives.formatDouble(d)
      case FloatType =>
        val f = row.getFloat(i)
        if (f.isNaN || f.isInfinity) Primitives.nullToken else Primitives.formatDouble(f.toDouble)
      case BooleanType    => Primitives.formatBoolean(row.getBoolean(i))
      case ByteType       => Primitives.formatLong(row.getByte(i).toLong)
      case ShortType      => Primitives.formatLong(row.getShort(i).toLong)
      case _: DecimalType => Primitives.formatBigDecimal(BigDecimal(row.getDecimal(i)))
      case DateType       =>
        Primitives.encodeStringLiteral(row.getAs[java.sql.Date](i).toString, delim)
      case TimestampType =>
        Primitives.encodeStringLiteral(row.getAs[java.sql.Timestamp](i).toInstant.toString, delim)
      case TimestampNTZType =>
        val text = row.get(i) match {
        case ldt: java.time.LocalDateTime => ldt.toString
        case ts: java.sql.Timestamp       => ts.toLocalDateTime.toString
        case other                        => other.toString
        }
        Primitives.encodeStringLiteral(text, delim)
      case BinaryType =>
        val encoded = java.util.Base64.getEncoder.encodeToString(row.getAs[Array[Byte]](i))
        Primitives.encodeStringLiteral(encoded, delim)
      case NullType => Primitives.nullToken
      case _        => Primitives.encodeStringLiteral(row.get(i).toString, delim)
      }

  private def encodeNestedChunkDataset(
      df: DataFrame,
      key: String,
      maxRowsPerChunk: Int,
      options: EncodeOptions,
  ): Dataset[String] = {
    val schema = df.schema
    df.mapPartitions { rows =>
      val fieldsWithIndex = schema.fields.zipWithIndex
      val chunkRows = new scala.collection.mutable.ArrayBuffer[JsonValue](maxRowsPerChunk)
      val encodedChunks = scala.collection.mutable.ArrayBuffer.empty[String]

      def flush(force: Boolean): Unit = {
        val shouldFlush = chunkRows.nonEmpty && (force || chunkRows.size >= maxRowsPerChunk)
        if (shouldFlush) {
          val wrappedChunk = JObj(VectorMap(key -> JArray(chunkRows.toVector)))
          encodeSafe(wrappedChunk, options) match {
          case Right(encoded) =>
            encodedChunks += encoded
            chunkRows.clear()
          case Left(err) =>
            throw new SparkToonEncodingException(err)
          }
        }
      }

      rows.foreach { row =>
        SparkJsonInterop.rowToJsonValueSafe(row, fieldsWithIndex) match {
        case Right(jsonRow) =>
          chunkRows += jsonRow
          flush(force = false)
        case Left(err) =>
          throw new SparkToonEncodingException(err)
        }
      }

      flush(force = true)
      encodedChunks.iterator
    }(Encoders.STRING)
  }

  private def collectToonChunks(
      chunksDataset: Dataset[String],
      key: String,
      options: EncodeOptions,
  ): Either[SparkToonError, Vector[String]] = {
    val maxChunks = SparkConfUtils.readPositiveInt(
      chunksDataset.sparkSession,
      MaxCollectedChunksConfKey,
      DefaultMaxCollectedChunks,
    )
    val maxPayloadBytes = SparkConfUtils.readPositiveLong(
      chunksDataset.sparkSession,
      MaxCollectedPayloadBytesConfKey,
      DefaultMaxCollectedPayloadBytes,
    )

    val collectedEither = Try(chunksDataset.take(maxChunks + 1).toVector).toEither.left.map { ex =>
      SparkToonError.CollectionError(
        s"Failed to collect TOON chunks: ${ex.getMessage}",
        Some(ex),
      )
    }

    collectedEither.flatMap { chunks =>
      if (chunks.size > maxChunks) {
        Left(
          SparkToonError.CollectionError(
            s"TOON chunk count exceeds collection safety limit ($maxChunks). " +
              s"Increase '$MaxCollectedChunksConfKey' or use toToonDataset."
          )
        )
      } else if (chunks.nonEmpty) {
        val totalBytes =
          chunks.foldLeft(0L)((acc, chunk) => acc + utf8ByteLength(chunk))
        if (totalBytes > maxPayloadBytes) {
          Left(
            SparkToonError.CollectionError(
              s"TOON payload size ($totalBytes bytes) exceeds collection safety limit " +
                s"($maxPayloadBytes bytes). Increase '$MaxCollectedPayloadBytesConfKey' or use toToonDataset."
            )
          )
        } else {
          Right(chunks)
        }
      } else {
        val wrappedChunk = JObj(VectorMap(key -> JArray(Vector.empty)))
        encodeSafe(wrappedChunk, options).map(encoded => Vector(encoded))
      }
    }
  }

  /**
   * Distributed partition-level metric calculation.
   *
   * Computes compact per-partition summaries and aggregates on driver.
   */
  private def calculateMetricsDistributed(
      df: DataFrame,
      key: String,
      maxRowsPerChunk: Int,
      options: EncodeOptions,
      tokenEstimator: ToonMetrics.TokenEstimator,
  ): Either[SparkToonError, ToonMetrics] = {
    if (maxRowsPerChunk <= 0) {
      Left(SparkToonError.ConversionError("maxRowsPerChunk must be greater than 0"))
    } else {
      val schema = df.schema
      val maxPartitionMetrics = SparkConfUtils.readPositiveInt(
        df.sparkSession,
        MaxCollectedPartitionMetricsConfKey,
        DefaultMaxCollectedPartitionMetrics,
      )
      val partitionMetricsDs = df.mapPartitions { rows =>
        val fieldsWithIndex = schema.fields.zipWithIndex
        val chunkRows = new scala.collection.mutable.ArrayBuffer[JsonValue](maxRowsPerChunk)
        var jsonTokenCount = 0
        var toonTokenCount = 0
        var rowCount = 0
        var error: Option[String] = None

        def flush(force: Boolean): Unit = {
          val shouldFlush = chunkRows.nonEmpty && (force || chunkRows.size >= maxRowsPerChunk)
          if (error.isEmpty && shouldFlush) {
            val wrappedChunk = JObj(VectorMap(key -> JArray(chunkRows.toVector)))
            val jsonCharEstimate = jsonCharCount(wrappedChunk)
            encodeSafe(wrappedChunk, options) match {
            case Right(toonEncoded) =>
              jsonTokenCount += tokenEstimator.estimate(jsonCharEstimate)
              toonTokenCount += tokenEstimator.estimate(toonEncoded)
              rowCount += chunkRows.size
              chunkRows.clear()
            case Left(err) =>
              error = Some(err.message)
            }
          }
        }

        rows.foreach { row =>
          if (error.isEmpty) {
            SparkJsonInterop.rowToJsonValueSafe(row, fieldsWithIndex) match {
            case Right(jsonRow) =>
              chunkRows += jsonRow
              flush(force = false)
            case Left(err) =>
              error = Some(err.message)
            }
          }
        }

        flush(force = true)
        Iterator.single(PartitionMetrics(
          jsonTokenCount = jsonTokenCount,
          toonTokenCount = toonTokenCount,
          rowCount = rowCount,
          columnCount = schema.fields.length,
          errorMessage = error,
        ))
      }(Encoders.product[PartitionMetrics])

      val aggregatedEither = Try {
        partitionMetricsDs
          .toDF()
          .agg(
            coalesce(sum(col("jsonTokenCount")), lit(0)).cast("long").as("json_tokens"),
            coalesce(sum(col("toonTokenCount")), lit(0)).cast("long").as("toon_tokens"),
            coalesce(sum(col("rowCount")), lit(0)).cast("long").as("rows"),
            coalesce(sum(lit(1)), lit(0)).cast("long").as("partition_count"),
            first(col("errorMessage"), ignoreNulls = true).as("first_error"),
          )
          .head()
      }.toEither.left.map { ex =>
        SparkToonError.CollectionError(
          s"Failed to aggregate distributed metrics: ${ex.getMessage}",
          Some(ex),
        )
      }

      aggregatedEither.flatMap { totalsRow =>
        val partitionCount = totalsRow.getLong(3)
        if (partitionCount > maxPartitionMetrics.toLong) {
          Left(SparkToonError.CollectionError(
            s"Partition metrics count exceeds safety limit ($maxPartitionMetrics). " +
              s"Increase '$MaxCollectedPartitionMetricsConfKey'."
          ))
        } else {
          val firstError = Option(totalsRow.getAs[String](4))
          firstError match {
          case Some(message) =>
            Left(SparkToonError.ConversionError(s"Failed to compute TOON metrics: $message"))
          case None =>
            for {
              jsonTokens <- boundedLongToInt(
                totalsRow.getLong(0),
                "jsonTokenCount",
                "Use smaller chunks or compute metrics on a narrower dataset slice.",
              )
              toonTokens <- boundedLongToInt(
                totalsRow.getLong(1),
                "toonTokenCount",
                "Use smaller chunks or compute metrics on a narrower dataset slice.",
              )
              rows <- boundedLongToInt(
                totalsRow.getLong(2),
                "rowCount",
                "Use partitioned metrics flow with bounded input size.",
              )
            } yield ToonMetrics(
              jsonTokenCount = jsonTokens,
              toonTokenCount = toonTokens,
              rowCount = rows,
              columnCount = schema.fields.length,
            )
          }
        }
      }
    }
  }

  private def boundedLongToInt(
      value: Long,
      field: String,
      guidance: String,
  ): Either[SparkToonError, Int] = {
    if (value > Int.MaxValue.toLong || value < Int.MinValue.toLong) {
      Left(SparkToonError.CollectionError(
        s"$field overflowed Int range while aggregating distributed metrics (value=$value). $guidance"
      ))
    } else Right(value.toInt)
  }

  private def writeChunksToLlmPartitions(
      chunks: Dataset[String],
      key: String,
      writerFactory: LlmPartitionWriterFactory,
      llmOptions: LlmPartitionWriteOptions,
  ): Either[SparkToonError, LlmPartitionWriteMetrics] = {
    val spark = chunks.sparkSession
    val partitionStatsDs = spark.createDataset(
      chunks.rdd.mapPartitionsWithIndex { (partitionId, chunkIterator) =>
        val writer = writerFactory.create()
        var chunkIndex = 0
        var attemptedChunks = 0L
        var sentChunks = 0L
        var duplicateChunks = 0L
        var failedChunks = 0L
        var totalBytes = 0L
        var totalEstimatedTokens = 0L
        var firstError: Option[String] = None

        def registerFailure(message: String): Unit = {
          failedChunks += 1L
          if (firstError.isEmpty) {
            firstError = Some(message)
          }
        }

        try {
          while (chunkIterator.hasNext && (!llmOptions.failOnError || firstError.isEmpty)) {
            val chunk = chunkIterator.next()
            attemptedChunks += 1L
            totalBytes += utf8ByteLength(chunk)
            totalEstimatedTokens += ToonMetrics.estimateTokens(chunk).toLong

            val idempotencyKey = LlmPartitionIdempotency.build(
              prefix = llmOptions.idempotencyPrefix,
              key = key,
              partitionId = partitionId,
              chunkIndex = chunkIndex,
              chunk = chunk,
            )
            val request = LlmChunkRequest(
              idempotencyKey = idempotencyKey,
              partitionId = partitionId,
              chunkIndex = chunkIndex,
              toonChunk = chunk,
            )
            val sendResult = LlmRetry.withRetries(
              maxRetries = llmOptions.maxRetries,
              backoffMs = llmOptions.retryBackoffMs,
            ) {
              writer.send(request)
            }

            sendResult match {
            case Right(LlmSendStatus.Sent) =>
              sentChunks += 1L
            case Right(LlmSendStatus.Duplicate) =>
              duplicateChunks += 1L
            case Left(err) =>
              registerFailure(err.formatted)
            }
            chunkIndex += 1
          }
        } catch {
          case NonFatal(ex) =>
            registerFailure(ex.getMessage)
        } finally {
          try writer.close()
          catch {
            case NonFatal(ex) =>
              if (firstError.isEmpty) {
                firstError = Some(s"Failed to close partition writer: ${ex.getMessage}")
              }
          }
        }

        Iterator.single(LlmPartitionMetrics(
          attemptedChunks = attemptedChunks,
          sentChunks = sentChunks,
          duplicateChunks = duplicateChunks,
          failedChunks = failedChunks,
          totalBytes = totalBytes,
          totalEstimatedTokens = totalEstimatedTokens,
          errorMessage = firstError,
        ))
      }
    )(Encoders.product[LlmPartitionMetrics])

    val aggregatedEither = Try {
      partitionStatsDs
        .toDF()
        .agg(
          coalesce(sum(col("attemptedChunks")), lit(0L)).cast("long").as("attempted_chunks"),
          coalesce(sum(col("sentChunks")), lit(0L)).cast("long").as("sent_chunks"),
          coalesce(sum(col("duplicateChunks")), lit(0L)).cast("long").as("duplicate_chunks"),
          coalesce(sum(col("failedChunks")), lit(0L)).cast("long").as("failed_chunks"),
          coalesce(sum(col("totalBytes")), lit(0L)).cast("long").as("total_bytes"),
          coalesce(sum(col("totalEstimatedTokens")), lit(0L)).cast("long").as("total_tokens"),
          coalesce(sum(lit(1L)), lit(0L)).cast("long").as("partition_count"),
          first(col("errorMessage"), ignoreNulls = true).as("first_error"),
        )
        .head()
    }.toEither.left.map { ex =>
      SparkToonError.CollectionError(
        s"Failed to aggregate LLM partition metrics: ${ex.getMessage}",
        Some(ex),
      )
    }

    aggregatedEither.flatMap { row =>
      val metrics = LlmPartitionWriteMetrics(
        partitionCount = row.getLong(6),
        attemptedChunks = row.getLong(0),
        sentChunks = row.getLong(1),
        duplicateChunks = row.getLong(2),
        failedChunks = row.getLong(3),
        totalBytes = row.getLong(4),
        totalEstimatedTokens = row.getLong(5),
        firstError = Option(row.getAs[String](7)),
      )

      if (llmOptions.failOnError && metrics.failedChunks > 0L) {
        Left(SparkToonError.ConversionError(
          s"Failed to write TOON chunks to LLM partitions: ${metrics.firstError.getOrElse("unknown error")}"
        ))
      } else {
        Right(metrics)
      }
    }
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
  private def extractRowsFromValue(value: JsonValue): Vector[JsonValue] = {
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

    findRowArray(value).getOrElse(Vector.empty)
  }

  /** Encode a JsonValue as a minimal JSON string (used in decode path). */
  private def encodeAsJson(value: JsonValue): String =
    SparkJsonInterop.encodeAsJson(value)

  /**
   * Estimate JSON character count from a JsonValue without materializing the string.
   *
   * Avoids allocating the full JSON string when only the length matters for token estimation.
   */
  private def jsonCharCount(value: JsonValue): Long = value match {
  case JsonValue.JNull          => 4L
  case JsonValue.JBool(b)       => if (b) 4L else 5L
  case JsonValue.JNumber(n)     => n.bigDecimal.stripTrailingZeros.toPlainString.length.toLong
  case JsonValue.JString(s)     => s.length.toLong + 2L
  case JsonValue.JArray(values) =>
    if (values.isEmpty) 2L
    else {
      var total = 2L // brackets
      total += (values.size - 1).toLong // commas
      var i = 0
      while (i < values.size) {
        total += jsonCharCount(values(i))
        i += 1
      }
      total
    }
  case JsonValue.JObj(fields) =>
    if (fields.isEmpty) 2L
    else {
      var total = 2L // braces
      total += (fields.size - 1).toLong // commas
      fields.foreach {
        case (k, v) =>
          total += k.length.toLong + 3L // key quotes + colon
          total += jsonCharCount(v)
      }
      total
    }
  }

  private def utf8ByteLength(s: String): Long = SparkConfUtils.utf8ByteLength(s)

}
