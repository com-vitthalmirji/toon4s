package io.toonformat.toon4s.spark.monitoring

import java.security.MessageDigest

import scala.util.Try

import io.toonformat.toon4s.spark.ToonMetrics
import io.toonformat.toon4s.spark.error.SparkToonError
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._

private[monitoring] object ToonMonitoringSupport {

  val MaxJsonFallbackRowsConfKey = "toon4s.spark.fallback.json.maxRows"
  val DefaultMaxJsonFallbackRows = 10000

  def estimateSavings(alignmentScore: Double): Double = {
    if (alignmentScore >= 0.9) 22.0
    else if (alignmentScore >= 0.8) 18.0
    else if (alignmentScore >= 0.7) 15.0
    else if (alignmentScore >= 0.5) 10.0
    else 0.0
  }

  def computeSchemaHash(schema: org.apache.spark.sql.types.StructType): String = {
    val schemaStr = schema.fields.map(f => s"${f.name}:${f.dataType.typeName}").mkString(",")
    val hash = MessageDigest.getInstance("MD5").digest(schemaStr.getBytes("UTF-8"))
    hash.map("%02x".format(_)).mkString.take(8)
  }

  def countRowsSafe(df: DataFrame): Option[Long] =
    Try(df.count()).toOption

  def summarizeChunkCounters(
      chunks: Vector[String],
      rowCount: Long,
  ): ToonMonitoring.EncodingCounters = {
    if (chunks.isEmpty) {
      ToonMonitoring.EncodingCounters(
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

      ToonMonitoring.EncodingCounters(
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

  def collectJsonFallbackSummary(
      df: DataFrame,
      configuredMaxRows: Int,
  ): Either[SparkToonError, ToonMonitoring.EncodingCounters] = {
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

        Right(ToonMonitoring.EncodingCounters(
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

  def formatBytes(bytes: Long): String = {
    if (bytes < 1024) s"$bytes bytes"
    else if (bytes < 1024 * 1024) f"${bytes / 1024.0}%.1f KB"
    else if (bytes < 1024 * 1024 * 1024) f"${bytes / (1024.0 * 1024.0)}%.1f MB"
    else f"${bytes / (1024.0 * 1024.0 * 1024.0)}%.1f GB"
  }

  private def readPositiveIntConf(
      spark: SparkSession,
      key: String,
      defaultValue: Int,
  ): Int =
    spark.conf
      .getOption(key)
      .flatMap(_.trim.toIntOption)
      .filter(_ > 0)
      .getOrElse(defaultValue)

}
