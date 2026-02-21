package io.toonformat.toon4s.spark.monitoring

import io.toonformat.toon4s.spark.{AdaptiveChunking, ToonMetrics}
import io.toonformat.toon4s.spark.SparkToonOps._
import org.apache.spark.sql.DataFrame

private[monitoring] object ToonPerformanceMeasurer {

  def measureEncodingPerformance(
      df: DataFrame,
      key: String,
      maxRowsPerChunk: Option[Int],
      tokenEstimator: ToonMetrics.TokenEstimator,
  ): ToonMonitoring.EncodingMetrics = {
    val startTime = System.currentTimeMillis()
    val metricsResult = df.toonMetricsWithEstimator(
      key = key,
      options = io.toonformat.toon4s.EncodeOptions(),
      tokenEstimator = tokenEstimator,
    )

    metricsResult match {
    case Right(toonMetrics) =>
      val chunkSize = maxRowsPerChunk.getOrElse {
        AdaptiveChunking.calculateOptimalChunkSize(df).chunkSize
      }
      val encodingResult = df.toToon(key = key, maxRowsPerChunk = chunkSize)
      val endTime = System.currentTimeMillis()

      encodingResult match {
      case Right(chunks) =>
        ToonMonitoring.EncodingMetrics(
          encodingTimeMs = endTime - startTime,
          jsonTokenCount = toonMetrics.jsonTokenCount,
          toonTokenCount = toonMetrics.toonTokenCount,
          savingsPercent = toonMetrics.savingsPercent,
          chunkCount = chunks.size,
          avgChunkSize = if (chunks.nonEmpty) toonMetrics.rowCount / chunks.size.toLong else 0L,
          success = true,
          errorType = None,
        )

      case Left(error) =>
        ToonMonitoring.EncodingMetrics(
          encodingTimeMs = endTime - startTime,
          jsonTokenCount = toonMetrics.jsonTokenCount,
          toonTokenCount = toonMetrics.toonTokenCount,
          savingsPercent = toonMetrics.savingsPercent,
          chunkCount = 0,
          avgChunkSize = 0L,
          success = false,
          errorType = Some(error.getClass.getSimpleName),
        )
      }

    case Left(error) =>
      val endTime = System.currentTimeMillis()
      ToonMonitoring.EncodingMetrics(
        encodingTimeMs = endTime - startTime,
        jsonTokenCount = 0L,
        toonTokenCount = 0L,
        savingsPercent = 0.0,
        chunkCount = 0,
        avgChunkSize = 0L,
        success = false,
        errorType = Some(error.getClass.getSimpleName),
      )
    }
  }

}
