package io.toonformat.toon4s.spark

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
   *   df.toJSON.collect() // Fall back to JSON
   * }
   *   }}}
   */
  def calculateOptimalChunkSize(df: DataFrame): ChunkingStrategy = {
    val rowCount = df.count()
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
    val totalDataSize = totalRows * avgRowSize

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
      val rowsPerChunk = math.max(100, TARGET_CHUNK_SIZE / avgRowSize)
      ChunkingStrategy(
        chunkSize = math.min(MAX_CHUNK_ROWS, rowsPerChunk),
        reasoning =
          s"Medium dataset (${formatBytes(totalDataSize)}). Large chunks amortize prompt tax.",
        useToon = true,
        estimatedDataSize = totalDataSize,
      )
    } else {
      // Large: TOON wins, use standard chunking
      val rowsPerChunk = TARGET_CHUNK_SIZE / avgRowSize
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
   * Combines alignment analysis and size analysis for fast go/no-go decision.
   *
   * @param df
   *   DataFrame to analyze
   * @return
   *   True if TOON recommended, false if JSON recommended
   */
  def shouldUseToon(df: DataFrame): Boolean = {
    import ToonAlignmentAnalyzer._

    val alignment = analyzeSchema(df.schema)
    val chunking = calculateOptimalChunkSize(df)

    alignment.aligned && chunking.useToon
  }

}
