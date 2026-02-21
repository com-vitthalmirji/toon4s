package io.toonformat.toon4s.spark.monitoring

import java.time.Instant

import io.toonformat.toon4s.spark.ToonAlignmentAnalyzer
import org.apache.spark.sql.DataFrame

private[monitoring] object ToonProductionReporter {

  def generateProductionReport(
      df: DataFrame,
      tableName: String,
  ): String = {
    val health = ToonHealthAssessor.assessDataFrameHealth(df, tableName)
    val telemetry = ToonHealthAssessor.collectTelemetry(df, tableName)
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
      s"- **Estimated size**: ${ToonMonitoringSupport.formatBytes(health.chunkStrategy.estimatedDataSize)}\n"
    )
    sb.append(s"- **Schema hash**: `${telemetry.schemaHash}`\n\n")

    sb.append(s"## Chunking strategy\n\n")
    sb.append(s"- **Use TOON**: ${if (health.chunkStrategy.useToon) "Yes" else "No"}\n")
    sb.append(s"- **Recommended chunk size**: ${health.chunkStrategy.chunkSize} rows\n")
    sb.append(s"- **Reasoning**: ${health.chunkStrategy.reasoning}\n")
    sb.append(s"- **Estimated token savings**: ~${health.estimatedSavings}%\n\n")

    sb.append(s"## Production readiness\n\n")
    if (health.productionReady) {
      sb.append(s"READY FOR PRODUCTION\n\n")
    } else {
      sb.append(s"NOT READY FOR PRODUCTION\n\n")
      sb.append(s"### Blocking issues\n\n")
      health.issues.foreach(issue => sb.append(s"- $issue\n"))
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
      sb.append(s"2. Use chunk size: `${health.chunkStrategy.chunkSize}` rows\n")
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

}
