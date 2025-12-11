package io.toonformat.toon4s.spark.monitoring

import io.toonformat.toon4s.spark.{AdaptiveChunking, ToonAlignmentAnalyzer, ToonMetrics}
import io.toonformat.toon4s.spark.error.SparkToonError
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.Instant

/**
 * Production monitoring utilities for TOON Spark integration.
 *
 * ==Why Monitor TOON Usage?==
 * Based on TOON Generation Benchmark findings, production deployments must track:
 *   - **Schema alignment drift**: Schemas change over time, affecting TOON accuracy
 *   - **Token efficiency**: Verify actual savings match benchmark expectations
 *   - **Prompt tax impact**: Monitor when prompt overhead exceeds syntax savings
 *   - **Error rates**: Track encoding failures and recovery patterns
 *
 * ==Key Metrics==
 *   1. **Alignment Score Trends**: Detect schema changes that reduce TOON effectiveness
 *   2. **Token Savings %**: Compare actual vs benchmark savings (22% for tabular data)
 *   3. **Chunk Size Distribution**: Optimize for prompt tax amortization
 *   4. **Error Frequency**: Identify non-recoverable encoding failures
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
 *   alertOncall(s"TOON health check failed: ${health.issues}")
 * }
 * }}}
 *
 * ==Integration with Monitoring Systems==
 * This module provides structured metrics for:
 *   - Datadog/New Relic (custom metrics)
 *   - Prometheus (gauge/counter exports)
 *   - CloudWatch (dimensions: table, environment)
 *   - Databricks Unity Catalog (audit logs)
 */
object ToonMonitoring {

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
   *   logger.error(s"TOON health check failed: ${health.issues}")
   *   df.toJSON.collect() // Fall back to JSON
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
      s"✅ $tableName: TOON ready (score=${alignment.score}, savings~${estimateSavings(alignment.score)}%)"
    } else {
      s"❌ $tableName: NOT TOON ready - ${issueList.mkString("; ")}"
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
   *   tags = Seq(s"table:${telemetry.tableName}"))
   * statsd.gauge("toon.max_depth", telemetry.maxDepth,
   *   tags = Seq(s"table:${telemetry.tableName}"))
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
   *   logger.warn(s"Low TOON savings: ${metrics.savingsPercent}% (expected 22%)")
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
    sb.append(s"- **TOON Aligned**: ${if (alignment.aligned) "✅ Yes" else "❌ No"}\n")
    sb.append(s"- **Max Nesting Depth**: ${alignment.maxDepth} levels\n")
    sb.append(s"- **Expected Accuracy**: ${alignment.expectedAccuracy}\n")
    sb.append(s"- **Recommendation**: ${alignment.recommendation}\n\n")

    if (alignment.warnings.nonEmpty) {
      sb.append(s"### Warnings\n\n")
      alignment.warnings.foreach { warning =>
        sb.append(s"- ⚠️ $warning\n")
      }
      sb.append("\n")
    }

    sb.append(s"## Dataset Characteristics\n\n")
    sb.append(s"- **Row Count**: ${telemetry.rowCount}\n")
    sb.append(s"- **Column Count**: ${telemetry.columnCount}\n")
    sb.append(
      s"- **Estimated Size**: ${formatBytes(health.chunkStrategy.estimatedDataSize)}\n"
    )
    sb.append(s"- **Schema Hash**: `${telemetry.schemaHash}`\n\n")

    sb.append(s"## Chunking Strategy\n\n")
    sb.append(s"- **Use TOON**: ${if (health.chunkStrategy.useToon) "✅ Yes" else "❌ No"}\n")
    sb.append(s"- **Recommended Chunk Size**: ${health.chunkStrategy.chunkSize} rows\n")
    sb.append(s"- **Reasoning**: ${health.chunkStrategy.reasoning}\n")
    sb.append(s"- **Estimated Token Savings**: ~${health.estimatedSavings}%\n\n")

    sb.append(s"## Production Readiness\n\n")
    if (health.productionReady) {
      sb.append(s"✅ **READY FOR PRODUCTION**\n\n")
    } else {
      sb.append(s"❌ **NOT READY FOR PRODUCTION**\n\n")
      sb.append(s"### Blocking Issues\n\n")
      health.issues.foreach { issue =>
        sb.append(s"- 🚫 $issue\n")
      }
      sb.append("\n")
    }

    if (health.warnings.nonEmpty) {
      sb.append(s"### Non-Blocking Warnings\n\n")
      health.warnings.foreach { warning =>
        sb.append(s"- ⚠️ $warning\n")
      }
      sb.append("\n")
    }

    sb.append(s"## Benchmark Comparison\n\n")
    sb.append(s"Based on [TOON Generation Benchmark](https://github.com/vetertann/TOON-generation-benchmark):\n\n")
    sb.append(s"- **Flat tabular** (depth 0-1): 90.5% accuracy, 22% token savings\n")
    sb.append(s"- **Shallow nesting** (depth 2): 78.6% accuracy, ~18% savings\n")
    sb.append(s"- **Medium nesting** (depth 3): 52.4% accuracy, ~12% savings\n")
    sb.append(s"- **Deep nesting** (depth 4+): 0% one-shot accuracy, NOT RECOMMENDED\n\n")
    sb.append(s"**Your schema** (depth ${alignment.maxDepth}): ${alignment.expectedAccuracy}\n\n")

    sb.append(s"## Recommendations\n\n")
    if (health.productionReady) {
      sb.append(s"1. ✅ Safe to deploy TOON encoding to production\n")
      sb.append(
        s"2. Use chunk size: `${health.chunkStrategy.chunkSize}` rows\n"
      )
      sb.append(s"3. Monitor token savings to verify ~${health.estimatedSavings}% reduction\n")
      sb.append(s"4. Set up alerting if alignment score drops below 0.7\n")
    } else {
      sb.append(s"1. ❌ Do NOT deploy TOON encoding yet\n")
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

  /**
   * Format bytes for human-readable output.
   */
  private def formatBytes(bytes: Long): String = {
    if (bytes < 1024) s"$bytes bytes"
    else if (bytes < 1024 * 1024) f"${bytes / 1024.0}%.1f KB"
    else if (bytes < 1024 * 1024 * 1024) f"${bytes / (1024.0 * 1024.0)}%.1f MB"
    else f"${bytes / (1024.0 * 1024.0 * 1024.0)}%.1f GB"
  }

}
