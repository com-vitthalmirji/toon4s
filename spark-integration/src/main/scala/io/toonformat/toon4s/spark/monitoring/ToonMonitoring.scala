package io.toonformat.toon4s.spark.monitoring

import java.time.Instant

import io.toonformat.toon4s.spark.{AdaptiveChunking, ToonMetrics, ToonSparkOptions}
import io.toonformat.toon4s.spark.error.SparkToonError
import org.apache.spark.sql.DataFrame

/**
 * Production monitoring utilities for TOON Spark integration.
 *
 * Public API stays in this object for compatibility. Implementations are split into focused
 * modules: health assessment, guardrailed encoding, performance measurements, and report rendering.
 */
object ToonMonitoring {

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
      maxJsonFallbackRows: Int = ToonMonitoringSupport.DefaultMaxJsonFallbackRows,
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

  final case class EncodingMetrics(
      encodingTimeMs: Long,
      jsonTokenCount: Long,
      toonTokenCount: Long,
      savingsPercent: Double,
      chunkCount: Int,
      avgChunkSize: Long,
      success: Boolean,
      errorType: Option[String],
  )

  def assessDataFrameHealth(
      df: DataFrame,
      tableName: String = "unknown",
  ): HealthAssessment =
    ToonHealthAssessor.assessDataFrameHealth(df, tableName)

  def collectTelemetry(
      df: DataFrame,
      tableName: String,
  ): TelemetrySnapshot =
    ToonHealthAssessor.collectTelemetry(df, tableName)

  def evaluateGuardrails(
      df: DataFrame,
      options: GuardrailOptions = GuardrailOptions(),
  ): GuardrailDecision =
    ToonGuardrailedEncoder.evaluateGuardrails(df, options)

  def encodeWithGuardrails(
      df: DataFrame,
      options: ToonSparkOptions = ToonSparkOptions.default,
      guardrailOptions: GuardrailOptions = GuardrailOptions(),
      fallbackMode: FallbackMode = FallbackMode.Json,
      onWarning: String => Unit = _ => (),
  ): Either[SparkToonError, GuardrailedEncodingResult] =
    ToonGuardrailedEncoder.encodeWithGuardrails(
      df,
      options,
      guardrailOptions,
      fallbackMode,
      onWarning,
    )

  def measureEncodingPerformance(
      df: DataFrame,
      key: String = "data",
      maxRowsPerChunk: Option[Int] = None,
  ): EncodingMetrics =
    ToonPerformanceMeasurer.measureEncodingPerformance(
      df = df,
      key = key,
      maxRowsPerChunk = maxRowsPerChunk,
      tokenEstimator = ToonMetrics.defaultTokenEstimator,
    )

  def measureEncodingPerformance(
      df: DataFrame,
      key: String,
      maxRowsPerChunk: Option[Int],
      tokenEstimator: ToonMetrics.TokenEstimator,
  ): EncodingMetrics =
    ToonPerformanceMeasurer.measureEncodingPerformance(
      df = df,
      key = key,
      maxRowsPerChunk = maxRowsPerChunk,
      tokenEstimator = tokenEstimator,
    )

  def generateProductionReport(
      df: DataFrame,
      tableName: String,
  ): String =
    ToonProductionReporter.generateProductionReport(df, tableName)

}
