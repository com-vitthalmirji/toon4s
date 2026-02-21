package io.toonformat.toon4s.spark.monitoring

import io.toonformat.toon4s.spark.{AdaptiveChunking, ToonAlignmentAnalyzer, ToonSparkOptions}
import io.toonformat.toon4s.spark.SparkToonOps._
import io.toonformat.toon4s.spark.error.SparkToonError
import org.apache.spark.sql.DataFrame

private[monitoring] object ToonGuardrailedEncoder {

  def evaluateGuardrails(
      df: DataFrame,
      options: ToonMonitoring.GuardrailOptions,
  ): ToonMonitoring.GuardrailDecision = {
    val alignment = ToonAlignmentAnalyzer.analyzeSchema(df.schema)
    val chunking = AdaptiveChunking.calculateOptimalChunkSize(df)
    val enoughDataForToon = chunking.estimatedDataSize >= options.minBytesPerChunk

    val useToon = options.mode match {
    case ToonMonitoring.GuardrailMode.Strict =>
      alignment.aligned && chunking.useToon && enoughDataForToon
    case ToonMonitoring.GuardrailMode.Lenient =>
      alignment.score >= 0.7 && enoughDataForToon
    }

    val reason =
      if (useToon) {
        s"TOON enabled (alignment=${alignment.score}, estimatedSize=${ToonMonitoringSupport.formatBytes(chunking.estimatedDataSize)})"
      } else {
        s"TOON blocked (alignment=${alignment.score}, estimatedSize=${ToonMonitoringSupport.formatBytes(chunking.estimatedDataSize)}, mode=${options.mode})"
      }

    ToonMonitoring.GuardrailDecision(
      useToon = useToon,
      mode = options.mode,
      alignmentScore = alignment.score,
      estimatedDataSize = chunking.estimatedDataSize,
      maxRowsPerChunk = options.maxRowsPerChunk,
      reason = reason,
    )
  }

  def encodeWithGuardrails(
      df: DataFrame,
      options: ToonSparkOptions,
      guardrailOptions: ToonMonitoring.GuardrailOptions,
      fallbackMode: ToonMonitoring.FallbackMode,
      onWarning: String => Unit,
  ): Either[SparkToonError, ToonMonitoring.GuardrailedEncodingResult] = {
    val effectiveOptions = options.copy(maxRowsPerChunk = guardrailOptions.maxRowsPerChunk)
    val decision = evaluateGuardrails(df, guardrailOptions)

    if (decision.useToon) {
      df.toToon(effectiveOptions).map { chunks =>
        val counters = ToonMonitoringSupport.summarizeChunkCounters(
          chunks,
          rowCount = ToonMonitoringSupport.countRowsSafe(df).getOrElse(0L),
        )
        ToonMonitoring.GuardrailedEncodingResult(
          path = ToonMonitoring.EncodingPath.Toon,
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
      case ToonMonitoring.FallbackMode.Json =>
        ToonMonitoringSupport.collectJsonFallbackSummary(df, guardrailOptions.maxJsonFallbackRows)
          .map { counters =>
            ToonMonitoring.GuardrailedEncodingResult(
              path = ToonMonitoring.EncodingPath.JsonFallback,
              toonChunks = Vector.empty,
              jsonFallbackRows = Vector.empty,
              counters = counters,
              decision = decision,
              warning = Some(warning),
            )
          }
      case ToonMonitoring.FallbackMode.Skip =>
        Right(ToonMonitoring.GuardrailedEncodingResult(
          path = ToonMonitoring.EncodingPath.Skipped,
          toonChunks = Vector.empty,
          jsonFallbackRows = Vector.empty,
          counters = ToonMonitoring.EncodingCounters(
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

}
