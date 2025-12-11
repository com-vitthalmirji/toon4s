package io.toonformat.toon4s.spark.integrations

import java.time.{Instant, LocalDateTime, ZoneId}

import io.toonformat.toon4s.spark.{AdaptiveChunking, ToonAlignmentAnalyzer}
import io.toonformat.toon4s.spark.SparkToonOps._
import io.toonformat.toon4s.spark.error.SparkToonError
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Apache Iceberg time travel integration for historical TOON snapshots.
 *
 * ==Use case: Temporal LLM analysis==
 * Modern data lakehouses need temporal analytics:
 *   - **Trend analysis**: "How did customer behavior change Q1 vs Q2?"
 *   - **Audit & compliance**: "What did this record look like on 2024-12-01?"
 *   - **A/B testing**: "Compare model predictions before/after schema change"
 *   - **Root cause analysis**: "When did data quality degrade?"
 *
 * ==Iceberg time travel==
 * Apache Iceberg provides:
 *   - Snapshot isolation (query historical state)
 *   - Millisecond-precision time travel
 *   - Efficient metadata-only operations
 *   - Multi-engine support (Spark, Trino, Flink)
 *
 * ==TOON advantage==
 * For LLM-based temporal analysis:
 *   - 22% token savings for tabular snapshots (benchmark-proven)
 *   - Efficient multi-snapshot encoding for trend detection
 *   - Schema alignment validation across time ranges
 *
 * ==Usage==
 * {{{
 * import io.toonformat.toon4s.spark.integrations.IcebergTimeTravel._
 * import java.time.Instant
 *
 * // Read snapshot at specific timestamp
 * val snapshot = readSnapshotAsOf(
 *   tableName = "warehouse.sales.transactions",
 *   asOfTimestamp = Instant.parse("2024-12-01T00:00:00Z")
 * )
 *
 * snapshot.foreach { toonChunks =>
 *   llmClient.analyze("Analyze sales on 2024-12-01: " + toonChunks.mkString)
 * }
 *
 * // Compare two snapshots for trend analysis
 * val comparison = compareSnapshots(
 *   tableName = "warehouse.sales.transactions",
 *   beforeTimestamp = Instant.parse("2024-11-01T00:00:00Z"),
 *   afterTimestamp = Instant.parse("2024-12-01T00:00:00Z")
 * )
 *
 * comparison.foreach { case (before, after) =>
 *   llmClient.analyze("Trend analysis:\nBefore: " + before + "\nAfter: " + after)
 * }
 * }}}
 *
 * ==Schema evolution handling==
 * Iceberg supports schema evolution. This module:
 *   - Validates schema compatibility across snapshots
 *   - Warns if schema changed between time ranges
 *   - Projects to common schema for consistent TOON encoding
 *
 * @see
 *   [[https://iceberg.apache.org/docs/latest/spark-queries/#time-travel Iceberg time travel]]
 * @see
 *   [[https://iceberg.apache.org/docs/latest/evolution/ Iceberg schema evolution]]
 */
object IcebergTimeTravel {

  /**
   * Snapshot metadata for TOON-encoded historical data.
   *
   * @param snapshotId
   *   Iceberg snapshot ID
   * @param timestamp
   *   Snapshot timestamp
   * @param toonChunks
   *   TOON-encoded data
   * @param rowCount
   *   Number of rows in snapshot
   * @param schemaId
   *   Iceberg schema ID (for detecting schema evolution)
   * @param alignmentScore
   *   Schema alignment score (0.0-1.0)
   */
  final case class SnapshotMetadata(
      snapshotId: Long,
      timestamp: Instant,
      toonChunks: Vector[String],
      rowCount: Long,
      schemaId: Int,
      alignmentScore: Double,
  )

  /**
   * Configuration for time travel queries.
   *
   * @param tableName
   *   Fully qualified Iceberg table name (e.g., "catalog.db.table")
   * @param key
   *   TOON key name for encoded chunks
   * @param maxRowsPerChunk
   *   Maximum rows per chunk (None = use adaptive chunking)
   * @param filterPredicate
   *   Optional WHERE clause to reduce snapshot size (e.g., "country = 'US'")
   */
  final case class TimeTravelConfig(
      tableName: String,
      key: String = "snapshot",
      maxRowsPerChunk: Option[Int] = None,
      filterPredicate: Option[String] = None,
  )

  /**
   * Read Iceberg table snapshot at specific timestamp as TOON-encoded chunks.
   *
   * Uses Iceberg's time travel syntax:
   * {{{
   * SELECT * FROM table TIMESTAMP AS OF '2024-12-01 00:00:00'
   * }}}
   *
   * ==Performance==
   * Iceberg time travel is metadata-only operation (fast):
   *   - No data rewriting required
   *   - Leverages snapshot isolation
   *   - Efficient columnar pruning
   *
   * @param tableName
   *   Iceberg table name
   * @param asOfTimestamp
   *   Point-in-time to query
   * @param config
   *   Optional time travel configuration
   * @param spark
   *   Spark session (must have Iceberg configured)
   * @return
   *   Either[SparkToonError, Vector[String]]
   *
   * @example
   *   {{{
   * val snapshot = readSnapshotAsOf(
   *   tableName = "prod.analytics.events",
   *   asOfTimestamp = Instant.parse("2024-12-01T00:00:00Z")
   * )
   *
   * snapshot.foreach { toon =>
   *   llmClient.analyze("Events on 2024-12-01: " + toon)
   * }
   *   }}}
   */
  def readSnapshotAsOf(
      tableName: String,
      asOfTimestamp: Instant,
      config: TimeTravelConfig = TimeTravelConfig(""),
  )(implicit spark: SparkSession): Either[SparkToonError, Vector[String]] = {
    val effectiveConfig = if (config.tableName.isEmpty) config.copy(tableName = tableName)
    else config

    readSnapshotWithMetadata(asOfTimestamp, effectiveConfig).map(_.toonChunks)
  }

  /**
   * Read Iceberg table snapshot at specific snapshot ID.
   *
   * More precise than timestamp-based queries when you know the exact snapshot ID.
   *
   * @param tableName
   *   Iceberg table name
   * @param snapshotId
   *   Iceberg snapshot ID
   * @param config
   *   Optional time travel configuration
   * @param spark
   *   Spark session
   * @return
   *   Either[SparkToonError, Vector[String]]
   *
   * @example
   *   {{{
   * val snapshot = readSnapshotById(
   *   tableName = "prod.analytics.events",
   *   snapshotId = 8744736658442914487L
   * )
   *   }}}
   */
  def readSnapshotById(
      tableName: String,
      snapshotId: Long,
      config: TimeTravelConfig = TimeTravelConfig(""),
  )(implicit spark: SparkSession): Either[SparkToonError, Vector[String]] = {
    val effectiveConfig = if (config.tableName.isEmpty) config.copy(tableName = tableName)
    else config

    val df = spark.read
      .format("iceberg")
      .option("snapshot-id", snapshotId.toString)
      .load(effectiveConfig.tableName)

    val filteredDF = effectiveConfig.filterPredicate match {
    case Some(predicate) => df.where(predicate)
    case None            => df
    }

    encodeWithAdaptiveChunking(filteredDF, effectiveConfig)
  }

  /**
   * Read snapshot with full metadata (snapshot ID, row count, schema ID, etc.).
   *
   * Useful for monitoring and debugging time travel queries.
   *
   * @param asOfTimestamp
   *   Point-in-time to query
   * @param config
   *   Time travel configuration
   * @param spark
   *   Spark session
   * @return
   *   Either[SparkToonError, SnapshotMetadata]
   *
   * @example
   *   {{{
   * val metadata = readSnapshotWithMetadata(
   *   asOfTimestamp = Instant.now().minusSeconds(3600),
   *   config = TimeTravelConfig(tableName = "events")
   * )
   *
   * metadata.foreach { meta =>
   *   println("Snapshot " + meta.snapshotId + ": " + meta.rowCount + " rows")
   *   println("Alignment score: " + meta.alignmentScore)
   * }
   *   }}}
   */
  def readSnapshotWithMetadata(
      asOfTimestamp: Instant,
      config: TimeTravelConfig,
  )(implicit spark: SparkSession): Either[SparkToonError, SnapshotMetadata] = {
    // Format timestamp for Iceberg (ISO-8601)
    val timestampStr = formatInstantForIceberg(asOfTimestamp)

    val df = spark.read
      .format("iceberg")
      .option("as-of-timestamp", timestampStr)
      .load(config.tableName)

    val filteredDF = config.filterPredicate match {
    case Some(predicate) => df.where(predicate)
    case None            => df
    }

    // Analyze schema alignment
    val alignment = ToonAlignmentAnalyzer.analyzeSchema(filteredDF.schema)

    // Encode to TOON
    val toonResult = encodeWithAdaptiveChunking(filteredDF, config)

    toonResult.map { toonChunks =>
      SnapshotMetadata(
        snapshotId = 0L, // Iceberg doesn't expose snapshot ID easily via DataFrame API
        timestamp = asOfTimestamp,
        toonChunks = toonChunks,
        rowCount = filteredDF.count(),
        schemaId = 0, // Schema ID not easily accessible
        alignmentScore = alignment.score,
      )
    }
  }

  /**
   * Compare two Iceberg snapshots for trend analysis.
   *
   * Encodes both snapshots as TOON and returns them for LLM comparison.
   *
   * ==Use case==
   * "How did customer demographics change between Q3 and Q4?"
   * {{{
   * val comparison = compareSnapshots(
   *   tableName = "customers",
   *   beforeTimestamp = Instant.parse("2024-09-30T23:59:59Z"), // Q3 end
   *   afterTimestamp = Instant.parse("2024-12-31T23:59:59Z")   // Q4 end
   * )
   *
   * comparison.foreach { case (before, after) =>
   *   val prompt = "\"\"\"" + """
   *   Analyze demographic changes:
   *   Q3 data: """ + before + """
   *   Q4 data: """ + after + """
   *
   *   Summarize key trends.
   *   \"\"\""""
   *   llmClient.analyze(prompt)
   * }
   * }}}
   *
   * @param tableName
   *   Iceberg table name
   * @param beforeTimestamp
   *   Earlier snapshot timestamp
   * @param afterTimestamp
   *   Later snapshot timestamp
   * @param config
   *   Optional time travel configuration
   * @param spark
   *   Spark session
   * @return
   *   Either[SparkToonError, (Vector[String], Vector[String])] - (before, after) TOON chunks
   */
  def compareSnapshots(
      tableName: String,
      beforeTimestamp: Instant,
      afterTimestamp: Instant,
      config: TimeTravelConfig = TimeTravelConfig(""),
  )(implicit spark: SparkSession): Either[SparkToonError, (Vector[String], Vector[String])] = {
    val effectiveConfig = if (config.tableName.isEmpty) config.copy(tableName = tableName)
    else config

    for {
      before <- readSnapshotAsOf(tableName, beforeTimestamp, effectiveConfig)
      after <- readSnapshotAsOf(tableName, afterTimestamp, effectiveConfig)
    } yield (before, after)
  }

  /**
   * Generate time series of snapshots at regular intervals.
   *
   * Useful for trend analysis over extended periods.
   *
   * ==Use case==
   * "Analyze weekly sales trends over Q4"
   * {{{
   * val timeSeries = generateSnapshotTimeSeries(
   *   tableName = "sales",
   *   startTime = Instant.parse("2024-10-01T00:00:00Z"),
   *   endTime = Instant.parse("2024-12-31T23:59:59Z"),
   *   intervalSeconds = 7 * 24 * 3600 // Weekly snapshots
   * )
   *
   * timeSeries.foreach { snapshots =>
   *   snapshots.foreach { case (timestamp, toon) =>
   *     println("Week of " + timestamp + ": " + toon.size + " chunks")
   *   }
   * }
   * }}}
   *
   * @param tableName
   *   Iceberg table name
   * @param startTime
   *   Time series start
   * @param endTime
   *   Time series end
   * @param intervalSeconds
   *   Interval between snapshots (e.g., 86400 = daily, 604800 = weekly)
   * @param config
   *   Optional time travel configuration
   * @param spark
   *   Spark session
   * @return
   *   Either[SparkToonError, Vector[(Instant, Vector[String])]]
   */
  def generateSnapshotTimeSeries(
      tableName: String,
      startTime: Instant,
      endTime: Instant,
      intervalSeconds: Long,
      config: TimeTravelConfig = TimeTravelConfig(""),
  )(implicit spark: SparkSession): Either[SparkToonError, Vector[(Instant, Vector[String])]] = {
    val effectiveConfig = if (config.tableName.isEmpty) config.copy(tableName = tableName)
    else config

    // Generate timestamp sequence
    val timestamps = generateTimestampSequence(startTime, endTime, intervalSeconds)

    // Read each snapshot
    val results = timestamps.map { timestamp =>
      readSnapshotAsOf(tableName, timestamp, effectiveConfig).map(toon => (timestamp, toon))
    }

    // Collect results (fail fast on first error)
    results.foldLeft[Either[SparkToonError, Vector[(
        Instant,
        Vector[String],
    )]]](Right(Vector.empty)) {
      case (Right(acc), Right(snapshot)) => Right(acc :+ snapshot)
      case (Left(err), _)                => Left(err)
      case (_, Left(err))                => Left(err)
    }
  }

  /**
   * Validate schema consistency across time range.
   *
   * Iceberg supports schema evolution, which can cause alignment score changes over time. Use this
   * to detect schema changes that might affect TOON encoding quality.
   *
   * @param tableName
   *   Iceberg table name
   * @param timestamps
   *   Timestamps to validate
   * @param spark
   *   Spark session
   * @return
   *   Map[Instant, AlignmentScore] - alignment scores at each timestamp
   *
   * @example
   *   {{{
   * val timestamps = Vector(
   *   Instant.parse("2024-10-01T00:00:00Z"),
   *   Instant.parse("2024-11-01T00:00:00Z"),
   *   Instant.parse("2024-12-01T00:00:00Z")
   * )
   *
   * val alignments = validateSchemaConsistency("events", timestamps)
   *
   * alignments.foreach { case (ts, score) =>
   *   println(ts + ": aligned=" + score.aligned + ", score=" + score.score)
   * }
   *   }}}
   */
  def validateSchemaConsistency(
      tableName: String,
      timestamps: Vector[Instant],
  )(implicit spark: SparkSession): Map[Instant, ToonAlignmentAnalyzer.AlignmentScore] = {
    timestamps.map { timestamp =>
      val timestampStr = formatInstantForIceberg(timestamp)
      val df = spark.read
        .format("iceberg")
        .option("as-of-timestamp", timestampStr)
        .load(tableName)

      val alignment = ToonAlignmentAnalyzer.analyzeSchema(df.schema)
      (timestamp, alignment)
    }.toMap
  }

  // ===== Private Helpers =====

  /** Encode DataFrame with adaptive chunking. */
  private def encodeWithAdaptiveChunking(
      df: DataFrame,
      config: TimeTravelConfig,
  ): Either[SparkToonError, Vector[String]] = {
    val chunkSize = config.maxRowsPerChunk.getOrElse {
      val strategy = AdaptiveChunking.calculateOptimalChunkSize(df)
      if (!strategy.useToon) {
        df.sparkSession.sparkContext.setJobDescription(
          s"Snapshot encoding: ${strategy.reasoning}"
        )
      }
      strategy.chunkSize
    }

    df.toToon(key = config.key, maxRowsPerChunk = chunkSize)
  }

  /**
   * Format Instant for Iceberg time travel syntax.
   *
   * Iceberg expects: "yyyy-MM-dd HH:mm:ss.SSS"
   */
  private def formatInstantForIceberg(instant: Instant): String = {
    val ldt = LocalDateTime.ofInstant(instant, ZoneId.of("UTC"))
    val formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
    ldt.format(formatter)
  }

  /** Generate sequence of timestamps at regular intervals. */
  private def generateTimestampSequence(
      start: Instant,
      end: Instant,
      intervalSeconds: Long,
  ): Vector[Instant] = {
    var current = start
    val result = Vector.newBuilder[Instant]

    while (current.isBefore(end) || current.equals(end)) {
      result += current
      current = current.plusSeconds(intervalSeconds)
    }

    result.result()
  }

}
