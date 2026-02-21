package io.toonformat.toon4s.spark.monitoring

import java.time.Instant

import io.toonformat.toon4s.spark.{AdaptiveChunking, ToonAlignmentAnalyzer}
import org.apache.spark.sql.DataFrame

private[monitoring] object ToonHealthAssessor {

  def assessDataFrameHealth(
      df: DataFrame,
      tableName: String,
  ): ToonMonitoring.HealthAssessment = {
    val alignment = ToonAlignmentAnalyzer.analyzeSchema(df.schema)
    val chunking = AdaptiveChunking.calculateOptimalChunkSize(df)

    val issues = List.newBuilder[String]
    val warnings = List.newBuilder[String]

    if (!alignment.aligned) {
      issues += s"Schema not TOON-aligned (score: ${alignment.score})"
    }

    if (!chunking.useToon) {
      issues += s"Chunking analysis recommends JSON: ${chunking.reasoning}"
    }

    if (alignment.maxDepth > 3) {
      issues += s"Deep nesting detected (${alignment.maxDepth} levels). Benchmark shows 0% one-shot accuracy."
    }

    if (alignment.score < 0.8 && alignment.aligned) {
      warnings += s"Low alignment score (${alignment.score}). Expected accuracy: ${alignment.expectedAccuracy}"
    }

    if (chunking.estimatedDataSize < 1024) {
      warnings += s"Small dataset (${ToonMonitoringSupport.formatBytes(chunking.estimatedDataSize)}). Prompt tax may exceed savings."
    }

    warnings ++= alignment.warnings

    val issueList = issues.result()
    val warningList = warnings.result()
    val productionReady = issueList.isEmpty

    val summary =
      if (productionReady)
        s"$tableName: TOON ready (score=${alignment.score}, savings~${ToonMonitoringSupport.estimateSavings(alignment.score)}%)"
      else s"$tableName: NOT TOON ready - ${issueList.mkString("; ")}"

    ToonMonitoring.HealthAssessment(
      alignmentScore = alignment.score,
      aligned = alignment.aligned,
      estimatedSavings = ToonMonitoringSupport.estimateSavings(alignment.score),
      chunkStrategy = chunking,
      productionReady = productionReady,
      issues = issueList,
      warnings = warningList,
      summary = summary,
    )
  }

  def collectTelemetry(
      df: DataFrame,
      tableName: String,
  ): ToonMonitoring.TelemetrySnapshot = {
    val schema = df.schema
    val alignment = ToonAlignmentAnalyzer.analyzeSchema(schema)
    val rowCount = df.count()
    val avgRowSize = AdaptiveChunking.estimateAvgRowSize(schema)
    val chunking = AdaptiveChunking.calculateOptimalChunkSize(rowCount, avgRowSize)

    ToonMonitoring.TelemetrySnapshot(
      timestamp = Instant.now(),
      tableName = tableName,
      rowCount = rowCount,
      columnCount = schema.fields.length,
      alignmentScore = alignment.score,
      maxDepth = alignment.maxDepth,
      estimatedDataSize = chunking.estimatedDataSize,
      recommendedChunkSize = chunking.chunkSize,
      useToon = chunking.useToon,
      schemaHash = ToonMonitoringSupport.computeSchemaHash(schema),
    )
  }

}
