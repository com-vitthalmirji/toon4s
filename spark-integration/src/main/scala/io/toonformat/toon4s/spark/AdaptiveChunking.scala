package io.toonformat.toon4s.spark

import scala.collection.immutable.VectorMap
import scala.util.Try

import io.toonformat.toon4s.{EncodeOptions, Toon}
import io.toonformat.toon4s.JsonValue._
import io.toonformat.toon4s.spark.internal.SparkConfUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

/**
 * Adaptive chunking strategy to optimize TOON prompt tax.
 *
 * ==Problem: The "Prompt Tax"==
 * TOON generation benchmarks revealed a critical issue:
 * {{{
 * TOON token savings = (JSON syntax savings) - (TOON prompt overhead)
 *
 * For SHORT outputs: prompt overhead > syntax savings = TOON LOSES
 * For LONG outputs: syntax savings > prompt overhead = TOON WINS
 * }}}
 *
 * ==Solution: Adaptive chunking==
 * Calculate optimal chunk size based on dataset characteristics to amortize prompt tax over larger
 * chunks.
 *
 * ==Usage==
 * {{{
 * import io.toonformat.toon4s.spark.AdaptiveChunking._
 *
 * val df = spark.sql("SELECT * FROM users")
 * val optimalChunkSize = calculateOptimalChunkSize(df)
 *
 * df.toToon(maxRowsPerChunk = optimalChunkSize) match {
 *   case Right(chunks) => // Process
 *   case Left(error) => // Handle
 * }
 * }}}
 *
 * ==Benchmark data==
 * Break-even analysis from benchmark:
 *   - Small dataset (< 1KB): JSON wins (prompt tax too high)
 *   - Medium dataset (1-10KB): TOON competitive
 *   - Large dataset (> 10KB): TOON wins (cumulative savings)
 */
object AdaptiveChunking {

  /**
   * Chunking strategy recommendation.
   *
   * @param chunkSize
   *   Recommended rows per chunk
   * @param reasoning
   *   Explanation of why this chunk size was chosen
   * @param useToToon
   *   Whether TOON is recommended (false = use JSON instead)
   * @param estimatedDataSize
   *   Estimated total data size in bytes
   */
  final case class ChunkingStrategy(
      chunkSize: Int,
      reasoning: String,
      useToon: Boolean,
      estimatedDataSize: Long,
  )

  /**
   * Calculate optimal chunk size for DataFrame.
   *
   * Analyzes DataFrame characteristics and recommends chunk size that amortizes TOON prompt tax.
   *
   * @param df
   *   DataFrame to analyze
   * @return
   *   ChunkingStrategy with recommended chunk size
   *
   * @example
   *   {{{
   * val df = spark.range(1000).toDF("id")
   * val strategy = calculateOptimalChunkSize(df)
   *
   * if (strategy.useToon) {
   *   df.toToon(maxRowsPerChunk = strategy.chunkSize)
   * } else {
   *   df.toJSON.take(1000) // Fall back to JSON with bounded sample
   * }
   *   }}}
   */
  def calculateOptimalChunkSize(df: DataFrame): ChunkingStrategy = {
    // Use logical plan stats when available to avoid a full count() scan.
    val stats = df.queryExecution.optimizedPlan.stats
    val rowCount = stats.rowCount.map(_.toLong).getOrElse(df.count())
    val avgRowSize = estimateAvgRowSize(df.schema)

    calculateOptimalChunkSize(rowCount, avgRowSize)
  }

  /**
   * Calculate optimal chunk size given row count and average row size.
   *
   * Based on benchmark findings:
   *   - Target chunk size: ~10KB (amortizes prompt tax)
   *   - Minimum dataset: 1KB (below this, JSON wins)
   *   - Maximum chunk: 1000 rows (prevents memory issues)
   *
   * @param totalRows
   *   Total number of rows in dataset
   * @param avgRowSize
   *   Average row size in bytes
   * @return
   *   ChunkingStrategy with recommendations
   */
  def calculateOptimalChunkSize(totalRows: Long, avgRowSize: Int): ChunkingStrategy = {
    val safeAvgRowSize = math.max(1, avgRowSize)
    val totalDataSize = safeMultiply(totalRows, safeAvgRowSize)

    // Benchmark break-even points
    val VERY_SMALL_THRESHOLD = 512 // < 512 bytes: JSON definitely wins
    val SMALL_THRESHOLD = 1024 // < 1KB: JSON likely wins
    val MEDIUM_THRESHOLD = 10 * 1024 // 1-10KB: TOON competitive
    val TARGET_CHUNK_SIZE = 10 * 1024 // 10KB per chunk (amortizes prompt tax)
    val MAX_CHUNK_ROWS = 1000 // Safety limit for memory

    if (totalDataSize < VERY_SMALL_THRESHOLD) {
      // Very small: JSON wins decisively
      ChunkingStrategy(
        chunkSize = Int.MaxValue, // Single chunk (but don't use TOON)
        reasoning =
          s"Dataset too small (${formatBytes(totalDataSize)}). Prompt tax > token savings. Use JSON.",
        useToon = false,
        estimatedDataSize = totalDataSize,
      )
    } else if (totalDataSize < SMALL_THRESHOLD) {
      // Small: JSON likely better
      val safeChunkRows =
        if (totalRows >= Int.MaxValue.toLong) Int.MaxValue
        else totalRows.toInt
      ChunkingStrategy(
        chunkSize = math.max(100, safeChunkRows),
        reasoning =
          s"Small dataset (${formatBytes(totalDataSize)}). TOON may not be worth prompt tax. Consider JSON.",
        useToon = false,
        estimatedDataSize = totalDataSize,
      )
    } else if (totalDataSize < MEDIUM_THRESHOLD) {
      // Medium: TOON competitive, use large chunks to amortize tax
      val rowsPerChunk = math.max(100, TARGET_CHUNK_SIZE / safeAvgRowSize)
      ChunkingStrategy(
        chunkSize = math.min(MAX_CHUNK_ROWS, rowsPerChunk),
        reasoning =
          s"Medium dataset (${formatBytes(totalDataSize)}). Large chunks amortize prompt tax.",
        useToon = true,
        estimatedDataSize = totalDataSize,
      )
    } else {
      // Large: TOON wins, use standard chunking
      val rowsPerChunk = TARGET_CHUNK_SIZE / safeAvgRowSize
      val optimalChunkSize = math.min(MAX_CHUNK_ROWS, math.max(100, rowsPerChunk))

      ChunkingStrategy(
        chunkSize = optimalChunkSize,
        reasoning =
          s"Large dataset (${formatBytes(totalDataSize)}). TOON wins via cumulative syntax savings.",
        useToon = true,
        estimatedDataSize = totalDataSize,
      )
    }
  }

  /**
   * Estimate average row size in bytes from schema.
   *
   * Conservative estimates for common types:
   *   - IntegerType: 4 bytes
   *   - LongType: 8 bytes
   *   - DoubleType: 8 bytes
   *   - StringType: 50 bytes (average string length)
   *   - BooleanType: 1 byte
   *   - Nested types: recursive calculation
   *
   * @param schema
   *   DataFrame schema
   * @return
   *   Estimated average row size in bytes
   */
  def estimateAvgRowSize(schema: StructType): Int = {
    schema.fields.map(f => estimateFieldSize(f.dataType)).sum
  }

  /**
   * Estimate field size in bytes for a DataType.
   *
   * @param dataType
   *   Spark DataType
   * @return
   *   Estimated size in bytes
   */
  private def estimateFieldSize(dataType: DataType): Int = dataType match {
  // Primitive types
  case ByteType | BooleanType                => 1
  case ShortType                             => 2
  case IntegerType | FloatType | DateType    => 4
  case LongType | DoubleType | TimestampType => 8
  case StringType                            => 50 // Conservative average
  case BinaryType                            => 100 // Conservative average
  case DecimalType()                         => 16
  case NullType                              => 0

  // Complex types (recursive)
  case ArrayType(elementType, _) =>
    // Assume average array size of 10 elements
    10 * estimateFieldSize(elementType)

  case StructType(fields) =>
    fields.map(f => estimateFieldSize(f.dataType)).sum

  case MapType(keyType, valueType, _) =>
    // Assume average map size of 5 entries
    5 * (estimateFieldSize(keyType) + estimateFieldSize(valueType))

  case _ => 50 // Unknown type, conservative estimate
  }

  /** Safe Long multiplication with saturation on overflow. */
  private def safeMultiply(a: Long, b: Int): Long = {
    val result = BigInt(a) * BigInt(b)
    if (result > BigInt(Long.MaxValue)) Long.MaxValue
    else if (result < BigInt(Long.MinValue)) Long.MinValue
    else result.toLong
  }

  /**
   * Format bytes for human-readable output.
   *
   * @param bytes
   *   Size in bytes
   * @return
   *   Formatted string (e.g., "1.5 KB", "10.2 MB")
   */
  private def formatBytes(bytes: Long): String = {
    if (bytes < 1024) s"$bytes bytes"
    else if (bytes < 1024 * 1024) f"${bytes / 1024.0}%.1f KB"
    else if (bytes < 1024 * 1024 * 1024) f"${bytes / (1024.0 * 1024.0)}%.1f MB"
    else f"${bytes / (1024.0 * 1024.0 * 1024.0)}%.1f GB"
  }

  /**
   * Quick check: Should we use TOON for this DataFrame?
   *
   * Combines alignment analysis, size analysis, and a sample-based efficiency probe. The probe
   * encodes a small sample in both JSON and TOON and compares byte counts. This catches cases where
   * the schema looks aligned but the actual data produces larger TOON output than JSON (e.g., flat
   * CSV aggregations with long string values).
   *
   * @param df
   *   DataFrame to analyze
   * @param key
   *   TOON document key (used for the probe encoding)
   * @param options
   *   TOON encoding options
   * @return
   *   True if TOON recommended, false if JSON recommended
   */
  def shouldUseToon(
      df: DataFrame,
      key: String = "data",
      options: EncodeOptions = EncodeOptions(),
  ): Boolean = {
    import ToonAlignmentAnalyzer._

    val alignment = analyzeSchema(df.schema)
    if (!alignment.aligned) return false

    val chunking = calculateOptimalChunkSize(df)
    if (!chunking.useToon) return false

    val probe = probeEfficiency(df, key = key, options = options)
    probe.toonEfficient
  }

  /**
   * Result of encoding a small sample in both JSON and TOON.
   *
   * @param sampleRows
   *   Number of rows in the sample
   * @param jsonBytes
   *   UTF-8 byte count of the JSON-encoded sample
   * @param toonBytes
   *   UTF-8 byte count of the TOON-encoded sample
   * @param toonEfficient
   *   True if TOON produced fewer or equal bytes than JSON
   * @param ratio
   *   toonBytes / jsonBytes. Values below 1.0 mean TOON wins.
   */
  final case class EfficiencyProbe(
      sampleRows: Int,
      jsonBytes: Long,
      toonBytes: Long,
      toonEfficient: Boolean,
      ratio: Double,
  )

  /** Default number of rows to sample when probing efficiency. */
  val DefaultProbeSampleSize = 20

  /**
   * Encode a small sample of rows in both JSON and TOON, then compare byte counts.
   *
   * This is a driver-side operation that takes a bounded sample. It should be called once per
   * DataFrame decision, not per partition.
   *
   * @param df
   *   DataFrame to probe
   * @param key
   *   TOON document key
   * @param sampleSize
   *   Maximum rows to sample (default 20)
   * @param options
   *   TOON encoding options
   * @return
   *   EfficiencyProbe with byte comparison
   */
  def probeEfficiency(
      df: DataFrame,
      key: String = "data",
      sampleSize: Int = DefaultProbeSampleSize,
      options: EncodeOptions = EncodeOptions(),
  ): EfficiencyProbe = {
    val effectiveSample = math.max(1, sampleSize)
    val collectedRows = Try(df.limit(effectiveSample).take(effectiveSample)).getOrElse(Array.empty)
    if (collectedRows.isEmpty) {
      return EfficiencyProbe(
        sampleRows = 0,
        jsonBytes = 0L,
        toonBytes = 0L,
        toonEfficient = false,
        ratio = 1.0,
      )
    }

    val schema = df.schema
    val fieldsWithIndex = schema.fields.zipWithIndex
    val jsonValues = collectedRows.map(row =>
      SparkJsonInterop.rowToJsonValueWithFields(row, fieldsWithIndex)
    )
    val wrapped = JObj(VectorMap(key -> JArray(jsonValues.toVector)))

    // JSON encoding from the same JsonValue tree
    val jsonEncoded = SparkJsonInterop.encodeAsJson(wrapped)
    val jsonBytes = SparkConfUtils.utf8ByteLength(jsonEncoded)

    // TOON encoding
    Toon.encode(wrapped, options) match {
    case Right(toonEncoded) =>
      val toonBytes = SparkConfUtils.utf8ByteLength(toonEncoded)
      val ratio = if (jsonBytes > 0L) toonBytes.toDouble / jsonBytes.toDouble else 1.0
      EfficiencyProbe(
        sampleRows = collectedRows.length,
        jsonBytes = jsonBytes,
        toonBytes = toonBytes,
        toonEfficient = ratio <= 1.0,
        ratio = ratio,
      )
    case Left(_) =>
      EfficiencyProbe(
        sampleRows = collectedRows.length,
        jsonBytes = jsonBytes,
        toonBytes = Long.MaxValue,
        toonEfficient = false,
        ratio = Double.MaxValue,
      )
    }
  }

}
