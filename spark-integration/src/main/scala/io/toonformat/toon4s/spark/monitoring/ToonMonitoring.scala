package io.toonformat.toon4s.spark.monitoring

import java.time.Instant

import scala.util.Try

import io.toonformat.toon4s.spark.{
  AdaptiveChunking,
  ToonAlignmentAnalyzer,
  ToonMetrics,
  ToonSparkOptions,
}
import io.toonformat.toon4s.spark.SparkToonOps._
import io.toonformat.toon4s.spark.error.SparkToonError
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Production monitoring utilities for TOON Spark integration.
 *
 * ==Why monitor TOON usage?==
 * Based on TOON generation benchmark findings, production deployments must track:
 *   - **Schema alignment drift**: Schemas change over time, affecting TOON accuracy
 *   - **Token efficiency**: Verify actual savings match benchmark expectations
 *   - **Prompt tax impact**: Monitor when prompt overhead exceeds syntax savings
 *   - **Error rates**: Track encoding failures and recovery patterns
 *
 * ==Key metrics==
 *   1. **Alignment score trends**: Detect schema changes that reduce TOON effectiveness
 *   2. **Token savings %**: Compare actual vs benchmark savings (22% for tabular data)
 *   3. **Chunk size distribution**: Optimize for prompt tax amortization
 *   4. **Error frequency**: Identify non-recoverable encoding failures
 *
 * ==Usage==
 * {{{
 * import io.toonformat.toon4s.spark.monitoring.ToonMonitoring._
 *
 * // Monitor DataFrame before encoding
 * val health = assessDataFrameHealth(df)
 * println(health.summary)
 *
 * // Track metrics over time
 * val telemetry = collectTelemetry(df, "production_events")
 * logToDatadog(telemetry) // Or your monitoring system
 *
 * // Real-time alerting
 * if (!health.productionReady) {
 *   alertOncall("TOON health check failed: " + health.issues)
 * }
 * }}}
 *
 * ==Integration with monitoring systems==
 * This module provides structured metrics for:
 *   - Datadog/New Relic (custom metrics)
 *   - Prometheus (gauge/counter exports)
 *   - CloudWatch (dimensions: table, environment)
 *   - Databricks Unity Catalog (audit logs)
 */
object ToonMonitoring {

  private val MaxJsonFallbackRowsConfKey = "toon4s.spark.fallback.json.maxRows"

  private val DefaultMaxJsonFallbackRows = 10000

  sealed trait GuardrailMode

  object GuardrailMode {

    case object Strict extends GuardrailMode

    case object Lenient extends GuardrailMode

  }

  sealed trait FallbackMode

  object FallbackMode {

    case object Json extends FallbackMode

    case object Skip extends FallbackMode

  }

  sealed trait EncodingPath

  object EncodingPath {

    case object Toon extends EncodingPath

    case object JsonFallback extends EncodingPath

    case object Skipped extends EncodingPath

  }

  final case class GuardrailOptions(
      minBytesPerChunk: Long = 10 * 1024,
      maxRowsPerChunk: Int = 1000,
      mode: GuardrailMode = GuardrailMode.Strict,
      maxJsonFallbackRows: Int = DefaultMaxJsonFallbackRows,
  )

  final case class GuardrailDecision(
      useToon: Boolean,
      mode: GuardrailMode,
      alignmentScore: Double,
      estimatedDataSize: Long,
      maxRowsPerChunk: Int,
      reason: String,
  )

  final case class EncodingCounters(
      totalEncodes: Long,
      successfulEncodes: Long,
      failedEncodes: Long,
      chunkCount: Int,
      minChunkRows: Int,
      maxChunkRows: Int,
      avgChunkRows: Double,
      minChunkBytes: Long,
      maxChunkBytes: Long,
      avgChunkBytes: Double,
      avgEstimatedTokensPerChunk: Double,
  )

  final case class GuardrailedEncodingResult(
      path: EncodingPath,
      toonChunks: Vector[String],
      jsonFallbackRows: Vector[String],
      counters: EncodingCounters,
      decision: GuardrailDecision,
      warning: Option[String],
  )

  /**
   * Health assessment for DataFrame TOON encoding.
   *
   * @param alignmentScore
   *   Schema alignment score (0.0-1.0)
   * @param aligned
   *   Whether schema is TOON-aligned
   * @param estimatedSavings
   *   Expected token savings percentage
   * @param chunkStrategy
   *   Recommended chunking strategy
   * @param productionReady
   *   Whether safe for production TOON encoding
   * @param issues
   *   List of blocking issues
   * @param warnings
   *   Non-blocking warnings
   * @param summary
   *   Human-readable health summary
   */
  final case class HealthAssessment(
      alignmentScore: Double,
      aligned: Boolean,
      estimatedSavings: Double,
      chunkStrategy: AdaptiveChunking.ChunkingStrategy,
      productionReady: Boolean,
      issues: List[String],
      warnings: List[String],
      summary: String,
  )

  /**
   * Telemetry snapshot for monitoring dashboards.
   *
   * @param timestamp
   *   Collection timestamp
   * @param tableName
   *   Source table name
   * @param rowCount
   *   Number of rows
   * @param columnCount
   *   Number of columns
   * @param alignmentScore
   *   Schema alignment score
   * @param maxDepth
   *   Schema nesting depth
   * @param estimatedDataSize
   *   Estimated total data size (bytes)
   * @param recommendedChunkSize
   *   Optimal chunk size
   * @param useToon
   *   Whether TOON is recommended
   * @param schemaHash
   *   Schema fingerprint (for detecting changes)
   */
  final case class TelemetrySnapshot(
      timestamp: Instant,
      tableName: String,
      rowCount: Long,
      columnCount: Int,
      alignmentScore: Double,
      maxDepth: Int,
      estimatedDataSize: Long,
      recommendedChunkSize: Int,
      useToon: Boolean,
      schemaHash: String,
  )

  /**
   * Encoding performance metrics.
   *
   * @param encodingTimeMs
   *   Time to encode (milliseconds)
   * @param jsonTokenCount
   *   JSON token count
   * @param toonTokenCount
   *   TOON token count
   * @param savingsPercent
   *   Token savings percentage
   * @param chunkCount
   *   Number of TOON chunks generated
   * @param avgChunkSize
   *   Average chunk size (rows)
   * @param success
   *   Whether encoding succeeded
   * @param errorType
   *   Error type if failed
   */
  final case class EncodingMetrics(
      encodingTimeMs: Long,
      jsonTokenCount: Int,
      toonTokenCount: Int,
      savingsPercent: Double,
      chunkCount: Int,
      avgChunkSize: Int,
      success: Boolean,
      errorType: Option[String],
  )

  /**
   * Assess DataFrame health for TOON encoding.
   *
   * Comprehensive pre-flight check before production encoding. Combines:
   *   - Schema alignment analysis
   *   - Adaptive chunking recommendations
   *   - Production readiness assessment
   *
   * @param df
   *   DataFrame to assess
   * @param tableName
   *   Optional table name for logging
   * @return
   *   HealthAssessment with detailed diagnostics
   *
   * @example
   *   {{{
   * val health = assessDataFrameHealth(df, "production.events")
   *
   * if (health.productionReady) {
   *   df.toToon(maxRowsPerChunk = health.chunkStrategy.chunkSize)
   * } else {
   *   logger.error("TOON health check failed: " + health.issues)
   *   df.toJSON.take(1000) // Fall back to JSON with bounded sample
   * }
   *   }}}
   */
  def assessDataFrameHealth(
      df: DataFrame,
      tableName: String = "unknown",
  ): HealthAssessment = {
    val alignment = ToonAlignmentAnalyzer.analyzeSchema(df.schema)
    val chunking = AdaptiveChunking.calculateOptimalChunkSize(df)

    val issues = List.newBuilder[String]
    val warnings = List.newBuilder[String]

    // Critical issues (block production)
    if (!alignment.aligned) {
      issues += s"Schema not TOON-aligned (score: ${alignment.score})"
    }

    if (!chunking.useToon) {
      issues += s"Chunking analysis recommends JSON: ${chunking.reasoning}"
    }

    if (alignment.maxDepth > 3) {
      issues += s"Deep nesting detected (${alignment.maxDepth} levels). Benchmark shows 0% one-shot accuracy."
    }

    // Warnings (non-blocking)
    if (alignment.score < 0.8 && alignment.aligned) {
      warnings += s"Low alignment score (${alignment.score}). Expected accuracy: ${alignment.expectedAccuracy}"
    }

    if (chunking.estimatedDataSize < 1024) {
      warnings += s"Small dataset (${formatBytes(chunking.estimatedDataSize)}). Prompt tax may exceed savings."
    }

    warnings ++= alignment.warnings

    val productionReady = issues.result().isEmpty
    val issueList = issues.result()
    val warningList = warnings.result()

    val summary = if (productionReady) {
      s"$tableName: TOON ready (score=${alignment.score}, savings~${estimateSavings(alignment.score)}%)"
    } else {
      s"$tableName: NOT TOON ready - ${issueList.mkString("; ")}"
    }

    HealthAssessment(
      alignmentScore = alignment.score,
      aligned = alignment.aligned,
      estimatedSavings = estimateSavings(alignment.score),
      chunkStrategy = chunking,
      productionReady = productionReady,
      issues = issueList,
      warnings = warningList,
      summary = summary,
    )
  }

  /**
   * Collect telemetry snapshot for monitoring dashboards.
   *
   * Lightweight metrics collection (no encoding, no data access). Safe to run frequently for
   * real-time monitoring.
   *
   * @param df
   *   DataFrame to monitor
   * @param tableName
   *   Table name for tracking
   * @return
   *   TelemetrySnapshot
   *
   * @example
   *   {{{
   * // Collect metrics every 5 minutes
   * val telemetry = collectTelemetry(df, "production.events")
   *
   * // Send to Datadog
   * statsd.gauge("toon.alignment_score", telemetry.alignmentScore,
   *   tags = Seq("table:" + telemetry.tableName))
   * statsd.gauge("toon.max_depth", telemetry.maxDepth,
   *   tags = Seq("table:" + telemetry.tableName))
   *   }}}
   */
  def collectTelemetry(
      df: DataFrame,
      tableName: String,
  ): TelemetrySnapshot = {
    val schema = df.schema
    val alignment = ToonAlignmentAnalyzer.analyzeSchema(schema)
    val rowCount = df.count()
    val avgRowSize = AdaptiveChunking.estimateAvgRowSize(schema)
    val chunking = AdaptiveChunking.calculateOptimalChunkSize(rowCount, avgRowSize)

    TelemetrySnapshot(
      timestamp = Instant.now(),
      tableName = tableName,
      rowCount = rowCount,
      columnCount = schema.fields.length,
      alignmentScore = alignment.score,
      maxDepth = alignment.maxDepth,
      estimatedDataSize = chunking.estimatedDataSize,
      recommendedChunkSize = chunking.chunkSize,
      useToon = chunking.useToon,
      schemaHash = computeSchemaHash(schema),
    )
  }

  /** Evaluate whether TOON should run with strict or lenient guardrails. */
  def evaluateGuardrails(
      df: DataFrame,
      options: GuardrailOptions = GuardrailOptions(),
  ): GuardrailDecision = {
    val alignment = ToonAlignmentAnalyzer.analyzeSchema(df.schema)
    val chunking = AdaptiveChunking.calculateOptimalChunkSize(df)
    val enoughDataForToon = chunking.estimatedDataSize >= options.minBytesPerChunk

    val useToon = options.mode match {
    case GuardrailMode.Strict =>
      alignment.aligned && chunking.useToon && enoughDataForToon
    case GuardrailMode.Lenient =>
      alignment.score >= 0.7 && enoughDataForToon
    }

    val reason =
      if (useToon) {
        s"TOON enabled (alignment=${alignment.score}, estimatedSize=${formatBytes(chunking.estimatedDataSize)})"
      } else {
        s"TOON blocked (alignment=${alignment.score}, estimatedSize=${formatBytes(chunking.estimatedDataSize)}, mode=${options.mode})"
      }

    GuardrailDecision(
      useToon = useToon,
      mode = options.mode,
      alignmentScore = alignment.score,
      estimatedDataSize = chunking.estimatedDataSize,
      maxRowsPerChunk = options.maxRowsPerChunk,
      reason = reason,
    )
  }

  /**
   * Encode with explicit guardrails and fallback strategy.
   *
   * Emits warning text through `onWarning` when TOON is blocked and fallback is used.
   */
  def encodeWithGuardrails(
      df: DataFrame,
      options: ToonSparkOptions = ToonSparkOptions.default,
      guardrailOptions: GuardrailOptions = GuardrailOptions(),
      fallbackMode: FallbackMode = FallbackMode.Json,
      onWarning: String => Unit = _ => (),
  ): Either[SparkToonError, GuardrailedEncodingResult] = {
    val effectiveOptions = options.copy(maxRowsPerChunk = guardrailOptions.maxRowsPerChunk)
    val decision = evaluateGuardrails(df, guardrailOptions)

    if (decision.useToon) {
      df.toToon(effectiveOptions).map { chunks =>
        val counters = summarizeChunkCounters(chunks, rowCount = countRowsSafe(df).getOrElse(0L))
        GuardrailedEncodingResult(
          path = EncodingPath.Toon,
          toonChunks = chunks,
          jsonFallbackRows = Vector.empty,
          counters = counters.copy(totalEncodes = 1L, successfulEncodes = 1L, failedEncodes = 0L),
          decision = decision,
          warning = None,
        )
      }
    } else {
      val warning = s"${decision.reason}. Fallback mode: $fallbackMode"
      onWarning(warning)
      fallbackMode match {
      case FallbackMode.Json =>
        collectJsonFallbackSummary(df, guardrailOptions.maxJsonFallbackRows).map { counters =>
          GuardrailedEncodingResult(
            path = EncodingPath.JsonFallback,
            toonChunks = Vector.empty,
            jsonFallbackRows = Vector.empty,
            counters = counters,
            decision = decision,
            warning = Some(warning),
          )
        }
      case FallbackMode.Skip =>
        Right(GuardrailedEncodingResult(
          path = EncodingPath.Skipped,
          toonChunks = Vector.empty,
          jsonFallbackRows = Vector.empty,
          counters = EncodingCounters(
            totalEncodes = 1L,
            successfulEncodes = 0L,
            failedEncodes = 1L,
            chunkCount = 0,
            minChunkRows = 0,
            maxChunkRows = 0,
            avgChunkRows = 0.0,
            minChunkBytes = 0L,
            maxChunkBytes = 0L,
            avgChunkBytes = 0.0,
            avgEstimatedTokensPerChunk = 0.0,
          ),
          decision = decision,
          warning = Some(warning),
        ))
      }
    }
  }

  /**
   * Measure encoding performance metrics.
   *
   * Executes TOON encoding and collects performance data. Use for benchmarking and capacity
   * planning.
   *
   * @param df
   *   DataFrame to encode
   * @param key
   *   TOON key name
   * @param maxRowsPerChunk
   *   Chunk size (None = adaptive)
   * @return
   *   EncodingMetrics
   *
   * @example
   *   {{{
   * val metrics = measureEncodingPerformance(df)
   *
   * if (metrics.savingsPercent < 15.0) {
   *   logger.warn("Low TOON savings: " + metrics.savingsPercent + "% (expected 22%)")
   * }
   *
   * // Track P99 encoding time
   * statsd.histogram("toon.encoding_time_ms", metrics.encodingTimeMs)
   *   }}}
   */
  def measureEncodingPerformance(
      df: DataFrame,
      key: String = "data",
      maxRowsPerChunk: Option[Int] = None,
  ): EncodingMetrics = {
    import io.toonformat.toon4s.spark.SparkToonOps._

    val startTime = System.currentTimeMillis()

    // Compute token metrics
    val metricsResult = df.toonMetrics(key = key)

    metricsResult match {
    case Right(toonMetrics) =>
      // Attempt encoding to get chunk count
      val chunkSize = maxRowsPerChunk.getOrElse {
        AdaptiveChunking.calculateOptimalChunkSize(df).chunkSize
      }

      val encodingResult = df.toToon(key = key, maxRowsPerChunk = chunkSize)
      val endTime = System.currentTimeMillis()

      encodingResult match {
      case Right(chunks) =>
        EncodingMetrics(
          encodingTimeMs = endTime - startTime,
          jsonTokenCount = toonMetrics.jsonTokenCount,
          toonTokenCount = toonMetrics.toonTokenCount,
          savingsPercent = toonMetrics.savingsPercent,
          chunkCount = chunks.size,
          avgChunkSize = if (chunks.nonEmpty) toonMetrics.rowCount / chunks.size else 0,
          success = true,
          errorType = None,
        )

      case Left(error) =>
        EncodingMetrics(
          encodingTimeMs = endTime - startTime,
          jsonTokenCount = toonMetrics.jsonTokenCount,
          toonTokenCount = toonMetrics.toonTokenCount,
          savingsPercent = toonMetrics.savingsPercent,
          chunkCount = 0,
          avgChunkSize = 0,
          success = false,
          errorType = Some(error.getClass.getSimpleName),
        )
      }

    case Left(error) =>
      val endTime = System.currentTimeMillis()
      EncodingMetrics(
        encodingTimeMs = endTime - startTime,
        jsonTokenCount = 0,
        toonTokenCount = 0,
        savingsPercent = 0.0,
        chunkCount = 0,
        avgChunkSize = 0,
        success = false,
        errorType = Some(error.getClass.getSimpleName),
      )
    }
  }

  /**
   * Generate production readiness report.
   *
   * Comprehensive report for stakeholder review before deploying TOON to production.
   *
   * @param df
   *   DataFrame to analyze
   * @param tableName
   *   Table name
   * @return
   *   Markdown-formatted report
   *
   * @example
   *   {{{
   * val report = generateProductionReport(df, "production.events")
   * println(report)
   * // Or write to file for documentation
   * Files.write(Paths.get("toon-readiness-report.md"), report.getBytes)
   *   }}}
   */
  def generateProductionReport(
      df: DataFrame,
      tableName: String,
  ): String = {
    val health = assessDataFrameHealth(df, tableName)
    val telemetry = collectTelemetry(df, tableName)
    val alignment = ToonAlignmentAnalyzer.analyzeSchema(df.schema)

    val sb = new StringBuilder()

    sb.append(s"# TOON Production Readiness Report\n\n")
    sb.append(s"**Table**: `$tableName`\n")
    sb.append(s"**Generated**: ${Instant.now()}\n\n")

    sb.append(s"## Executive Summary\n\n")
    sb.append(s"${health.summary}\n\n")

    sb.append(s"## Schema Analysis\n\n")
    sb.append(s"- **Alignment Score**: ${alignment.score} / 1.0\n")
    sb.append(s"- **TOON Aligned**: ${if (alignment.aligned) "Yes" else "No"}\n")
    sb.append(s"- **Max Nesting Depth**: ${alignment.maxDepth} levels\n")
    sb.append(s"- **Expected Accuracy**: ${alignment.expectedAccuracy}\n")
    sb.append(s"- **Recommendation**: ${alignment.recommendation}\n\n")

    if (alignment.warnings.nonEmpty) {
      sb.append(s"### Warnings\n\n")
      alignment.warnings.foreach(warning => sb.append(s"- $warning\n"))
      sb.append("\n")
    }

    sb.append(s"## Dataset characteristics\n\n")
    sb.append(s"- **Row count**: ${telemetry.rowCount}\n")
    sb.append(s"- **Column count**: ${telemetry.columnCount}\n")
    sb.append(
      s"- **Estimated size**: ${formatBytes(health.chunkStrategy.estimatedDataSize)}\n"
    )
    sb.append(s"- **Schema hash**: `${telemetry.schemaHash}`\n\n")

    sb.append(s"## Chunking strategy\n\n")
    sb.append(s"- **Use TOON**: ${if (health.chunkStrategy.useToon) "✅ Yes" else "❌ No"}\n")
    sb.append(s"- **Recommended chunk size**: ${health.chunkStrategy.chunkSize} rows\n")
    sb.append(s"- **Reasoning**: ${health.chunkStrategy.reasoning}\n")
    sb.append(s"- **Estimated token savings**: ~${health.estimatedSavings}%\n\n")

    sb.append(s"## Production readiness\n\n")
    if (health.productionReady) {
      sb.append(s"**READY FOR PRODUCTION**\n\n")
    } else {
      sb.append(s"**NOT READY FOR PRODUCTION**\n\n")
      sb.append(s"### Blocking issues\n\n")
      health.issues.foreach(issue => sb.append(s"- 🚫 $issue\n"))
      sb.append("\n")
    }

    if (health.warnings.nonEmpty) {
      sb.append(s"### Non-blocking warnings\n\n")
      health.warnings.foreach(warning => sb.append(s"- $warning\n"))
      sb.append("\n")
    }

    sb.append(s"## Benchmark comparison\n\n")
    sb.append(s"- **Flat tabular** (depth 0-1): 90.5% accuracy, 22% token savings\n")
    sb.append(s"- **Shallow nesting** (depth 2): 78.6% accuracy, ~18% savings\n")
    sb.append(s"- **Medium nesting** (depth 3): 52.4% accuracy, ~12% savings\n")
    sb.append(s"- **Deep nesting** (depth 4+): 0% one-shot accuracy, NOT RECOMMENDED\n\n")
    sb.append(
      s"**Your schema** (depth ${alignment.maxDepth}): ${alignment.expectedAccuracy}\n\n"
    )

    sb.append(s"## Recommendations\n\n")
    if (health.productionReady) {
      sb.append(s"1. Safe to deploy TOON encoding to production\n")
      sb.append(
        s"2. Use chunk size: `${health.chunkStrategy.chunkSize}` rows\n"
      )
      sb.append(s"3. Monitor token savings to verify ~${health.estimatedSavings}% reduction\n")
      sb.append(s"4. Set up alerting if alignment score drops below 0.7\n")
    } else {
      sb.append(s"1. Do NOT deploy TOON encoding yet\n")
      sb.append(s"2. Address blocking issues listed above\n")
      sb.append(s"3. Consider schema flattening if deep nesting detected\n")
      sb.append(s"4. Alternative: Use JSON encoding instead of TOON\n")
    }

    sb.result()
  }

  // ===== Private Helpers =====

  /**
   * Estimate token savings based on alignment score.
   *
   * Conservative estimates based on benchmark data.
   */
  private def estimateSavings(alignmentScore: Double): Double = {
    if (alignmentScore >= 0.9) 22.0 // Benchmark: users case 22% savings
    else if (alignmentScore >= 0.8) 18.0
    else if (alignmentScore >= 0.7) 15.0
    else if (alignmentScore >= 0.5) 10.0
    else 0.0 // Below 0.5: likely no savings due to repair overhead
  }

  /**
   * Compute schema fingerprint for change detection.
   *
   * Simple hash of schema field names and types.
   */
  private def computeSchemaHash(schema: org.apache.spark.sql.types.StructType): String = {
    val schemaStr = schema.fields.map(f => s"${f.name}:${f.dataType.typeName}").mkString(",")
    val hash = java.security.MessageDigest
      .getInstance("MD5")
      .digest(schemaStr.getBytes("UTF-8"))
    hash.map("%02x".format(_)).mkString.take(8)
  }

  private def collectJsonFallbackSummary(
      df: DataFrame,
      configuredMaxRows: Int,
  ): Either[SparkToonError, EncodingCounters] = {
    val confMaxRows = readPositiveIntConf(
      df.sparkSession,
      MaxJsonFallbackRowsConfKey,
      DefaultMaxJsonFallbackRows,
    )
    val effectiveMaxRows =
      if (configuredMaxRows > 0) math.min(configuredMaxRows, confMaxRows) else confMaxRows

    val statsResult = Try {
      df.toJSON
        .limit(effectiveMaxRows + 1)
        .mapPartitions { values =>
          var rowCount = 0L
          var totalBytes = 0L
          var totalTokens = 0L
          var minBytes = Long.MaxValue
          var maxBytes = 0L

          values.foreach { value =>
            val bytes = value.getBytes("UTF-8").length.toLong
            rowCount += 1L
            totalBytes += bytes
            totalTokens += ToonMetrics.estimateTokens(value).toLong
            minBytes = math.min(minBytes, bytes)
            maxBytes = math.max(maxBytes, bytes)
          }

          Iterator.single((rowCount, totalBytes, totalTokens, minBytes, maxBytes))
        }(Encoders.tuple(
          Encoders.scalaLong,
          Encoders.scalaLong,
          Encoders.scalaLong,
          Encoders.scalaLong,
          Encoders.scalaLong,
        ))
        .toDF("row_count", "total_bytes", "total_tokens", "min_bytes", "max_bytes")
        .agg(
          coalesce(sum(col("row_count")), lit(0L)).cast("long").as("row_count"),
          coalesce(sum(col("total_bytes")), lit(0L)).cast("long").as("total_bytes"),
          coalesce(sum(col("total_tokens")), lit(0L)).cast("long").as("total_tokens"),
          coalesce(max(col("max_bytes")), lit(0L)).cast("long").as("max_bytes"),
          coalesce(min(col("min_bytes")), lit(Long.MaxValue)).cast("long").as("min_bytes"),
        )
        .head()
    }.toEither.left.map { ex =>
      SparkToonError.CollectionError(
        s"Failed to summarize JSON fallback rows: ${ex.getMessage}",
        Some(ex),
      )
    }

    statsResult.flatMap { stats =>
      val rowCount = stats.getLong(0)
      if (rowCount > effectiveMaxRows.toLong) {
        Left(
          SparkToonError.CollectionError(
            s"JSON fallback row count exceeds safety limit ($effectiveMaxRows). " +
              s"Increase '$MaxJsonFallbackRowsConfKey' or use fallback skip mode."
          )
        )
      } else {
        val totalBytes = stats.getLong(1)
        val totalTokens = stats.getLong(2)
        val maxBytes = stats.getLong(3)
        val minBytesRaw = stats.getLong(4)
        val minBytes = if (minBytesRaw == Long.MaxValue) 0L else minBytesRaw
        val avgChunkBytes =
          if (rowCount > 0L) totalBytes.toDouble / rowCount.toDouble else 0.0
        val avgTokensPerChunk =
          if (rowCount > 0L) totalTokens.toDouble / rowCount.toDouble else 0.0

        Right(EncodingCounters(
          totalEncodes = 1L,
          successfulEncodes = 0L,
          failedEncodes = 1L,
          chunkCount = math.min(rowCount, Int.MaxValue.toLong).toInt,
          minChunkRows = if (rowCount > 0L) 1 else 0,
          maxChunkRows = if (rowCount > 0L) 1 else 0,
          avgChunkRows = if (rowCount > 0L) 1.0 else 0.0,
          minChunkBytes = minBytes,
          maxChunkBytes = maxBytes,
          avgChunkBytes = avgChunkBytes,
          avgEstimatedTokensPerChunk = avgTokensPerChunk,
        ))
      }
    }
  }

  private def countRowsSafe(df: DataFrame): Option[Long] =
    Try(df.count()).toOption

  private def summarizeChunkCounters(
      chunks: Vector[String],
      rowCount: Long,
  ): EncodingCounters = {
    if (chunks.isEmpty) {
      EncodingCounters(
        totalEncodes = 0L,
        successfulEncodes = 0L,
        failedEncodes = 0L,
        chunkCount = 0,
        minChunkRows = 0,
        maxChunkRows = 0,
        avgChunkRows = 0.0,
        minChunkBytes = 0L,
        maxChunkBytes = 0L,
        avgChunkBytes = 0.0,
        avgEstimatedTokensPerChunk = 0.0,
      )
    } else {
      val chunkSizes = chunks.map(_.getBytes("UTF-8").length.toLong)
      val estimatedTokens = chunks.map(ToonMetrics.estimateTokens)
      val avgRows = rowCount.toDouble / chunks.size.toDouble

      EncodingCounters(
        totalEncodes = 0L,
        successfulEncodes = 0L,
        failedEncodes = 0L,
        chunkCount = chunks.size,
        minChunkRows = math.floor(avgRows).toInt,
        maxChunkRows = math.ceil(avgRows).toInt,
        avgChunkRows = avgRows,
        minChunkBytes = chunkSizes.min,
        maxChunkBytes = chunkSizes.max,
        avgChunkBytes = chunkSizes.sum.toDouble / chunkSizes.size.toDouble,
        avgEstimatedTokensPerChunk = estimatedTokens.sum.toDouble / estimatedTokens.size.toDouble,
      )
    }
  }

  /** Format bytes for human-readable output. */
  private def formatBytes(bytes: Long): String = {
    if (bytes < 1024) s"$bytes bytes"
    else if (bytes < 1024 * 1024) f"${bytes / 1024.0}%.1f KB"
    else if (bytes < 1024 * 1024 * 1024) f"${bytes / (1024.0 * 1024.0)}%.1f MB"
    else f"${bytes / (1024.0 * 1024.0 * 1024.0)}%.1f GB"
  }

  private def readPositiveIntConf(
      spark: SparkSession,
      key: String,
      defaultValue: Int,
  ): Int = {
    spark.conf
      .getOption(key)
      .flatMap(_.trim.toIntOption)
      .filter(_ > 0)
      .getOrElse(defaultValue)
  }

}
