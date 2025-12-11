package examples

import io.toonformat.toon4s.spark.SparkToonOps._
import io.toonformat.toon4s.spark.monitoring.ToonMonitoring._
import io.toonformat.toon4s.spark.{AdaptiveChunking, ToonAlignmentAnalyzer}
import org.apache.spark.sql.SparkSession

/**
 * Production monitoring example: Pre-deployment health checks and runtime telemetry.
 *
 * ==Use Case==
 * Before deploying TOON encoding to production, validate:
 *   1. Schema alignment (avoid encoding failures)
 *   2. Token savings meet expectations (ROI validation)
 *   3. Prompt tax optimization (chunk size tuning)
 *   4. Runtime monitoring setup (alerting, dashboards)
 *
 * ==Workflow==
 * ```
 * Development Phase:
 *   1. Generate production readiness report
 *   2. Validate schema alignment
 *   3. Measure baseline token savings
 *
 * Deployment Phase:
 *   4. Collect telemetry snapshots
 *   5. Track encoding performance
 *   6. Set up alerting thresholds
 *
 * Production Phase:
 *   7. Monitor alignment score drift
 *   8. Track token savings vs benchmark
 *   9. Alert on encoding failures
 * ```
 *
 * ==Running==
 * {{{
 * spark-submit --class examples.ProductionMonitoringExample \
 *   --master yarn \
 *   --deploy-mode client \
 *   toon4s-spark-assembly.jar
 * }}}
 */
object ProductionMonitoringExample {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("TOON Production Monitoring")
      .getOrCreate()

    import spark.implicits._

    println("=== TOON Production Monitoring Demo ===")

    // Simulate production tables with varying characteristics
    val tables = Map(
      "production.user_events" -> createMockUserEvents(),
      "production.nested_orders" -> createMockNestedOrders(),
      "production.deep_hierarchy" -> createMockDeepHierarchy(),
    )

    // ===== Phase 1: Pre-Deployment Validation =====
    println("\n" + "=" * 60)
    println("PHASE 1: PRE-DEPLOYMENT VALIDATION")
    println("=" * 60)

    tables.foreach { case (tableName, df) =>
      println(s"\n>>> Validating: $tableName")

      // Step 1: Health assessment
      val health = assessDataFrameHealth(df, tableName)
      println(s"\n[Health Assessment]")
      println(health.summary)

      if (!health.productionReady) {
        println("\n❌ BLOCKING ISSUES:")
        health.issues.foreach(issue => println(s"  - $issue"))
      }

      if (health.warnings.nonEmpty) {
        println("\n⚠️ WARNINGS:")
        health.warnings.foreach(warning => println(s"  - $warning"))
      }

      // Step 2: Generate full production report
      val report = generateProductionReport(df, tableName)
      val reportPath = s"/tmp/toon-report-${tableName.replace(".", "_")}.md"

      import java.nio.file.{Files, Paths}
      Files.write(Paths.get(reportPath), report.getBytes)
      println(s"\n📄 Full report saved: $reportPath")

      // Step 3: Decision: Deploy or not?
      if (health.productionReady) {
        println(s"\n✅ APPROVED FOR PRODUCTION")
        println(s"   Recommended chunk size: ${health.chunkStrategy.chunkSize} rows")
        println(s"   Expected token savings: ~${health.estimatedSavings}%")
      } else {
        println(s"\n🚫 NOT APPROVED - Fix issues before deployment")
        println(s"   Recommendation: Use JSON encoding instead of TOON")
      }
    }

    // ===== Phase 2: Baseline Performance Measurement =====
    println("\n" + "=" * 60)
    println("PHASE 2: BASELINE PERFORMANCE MEASUREMENT")
    println("=" * 60)

    tables.foreach { case (tableName, df) =>
      println(s"\n>>> Measuring: $tableName")

      val metrics = measureEncodingPerformance(df, key = tableName)

      println(s"\n[Performance Metrics]")
      println(s"  Encoding time: ${metrics.encodingTimeMs}ms")
      println(s"  JSON tokens: ${metrics.jsonTokenCount}")
      println(s"  TOON tokens: ${metrics.toonTokenCount}")
      println(f"  Savings: ${metrics.savingsPercent}%.2f%%")
      println(s"  Chunks: ${metrics.chunkCount}")
      println(s"  Avg chunk size: ${metrics.avgChunkSize} rows")
      println(s"  Success: ${metrics.success}")

      // Compare to benchmark expectations
      val expectedSavings = 22.0 // From TOON benchmark
      val variance = math.abs(metrics.savingsPercent - expectedSavings)

      if (variance > 10.0) {
        println(
          f"\n⚠️ Savings variance from benchmark: ${variance}%.1f%% (expected: $expectedSavings%%, actual: ${metrics.savingsPercent}%.1f%%)"
        )
      } else {
        println(f"\n✅ Savings align with benchmark (expected: $expectedSavings%%, actual: ${metrics.savingsPercent}%.1f%%)")
      }
    }

    // ===== Phase 3: Runtime Telemetry Collection =====
    println("\n" + "=" * 60)
    println("PHASE 3: RUNTIME TELEMETRY COLLECTION")
    println("=" * 60)

    tables.foreach { case (tableName, df) =>
      println(s"\n>>> Collecting telemetry: $tableName")

      val telemetry = collectTelemetry(df, tableName)

      println(s"\n[Telemetry Snapshot]")
      println(s"  Timestamp: ${telemetry.timestamp}")
      println(s"  Rows: ${telemetry.rowCount}")
      println(s"  Columns: ${telemetry.columnCount}")
      println(f"  Alignment score: ${telemetry.alignmentScore}%.2f")
      println(s"  Max depth: ${telemetry.maxDepth}")
      println(s"  Estimated size: ${formatBytes(telemetry.estimatedDataSize)}")
      println(s"  Recommended chunk: ${telemetry.recommendedChunkSize} rows")
      println(s"  Use TOON: ${telemetry.useToon}")
      println(s"  Schema hash: ${telemetry.schemaHash}")

      // Simulate sending to monitoring system
      sendToDatadog(telemetry)
      sendToPrometheus(telemetry)
    }

    // ===== Phase 4: Alerting Configuration =====
    println("\n" + "=" * 60)
    println("PHASE 4: ALERTING THRESHOLDS")
    println("=" * 60)

    tables.foreach { case (tableName, df) =>
      println(s"\n>>> Configuring alerts: $tableName")

      val health = assessDataFrameHealth(df, tableName)
      val telemetry = collectTelemetry(df, tableName)

      // Define alerting thresholds
      val thresholds = Map(
        "alignment_score_min" -> 0.7,
        "token_savings_min" -> 15.0,
        "encoding_failure_rate_max" -> 0.05,
        "max_depth" -> 3,
      )

      println(s"\n[Alert Configuration]")
      thresholds.foreach { case (metric, threshold) =>
        println(s"  $metric: $threshold")
      }

      // Check current values against thresholds
      val alerts = List.newBuilder[String]

      if (telemetry.alignmentScore < thresholds("alignment_score_min")) {
        alerts += s"🚨 CRITICAL: Alignment score ${telemetry.alignmentScore} below threshold ${thresholds("alignment_score_min")}"
      }

      if (telemetry.maxDepth > thresholds("max_depth")) {
        alerts += s"🚨 CRITICAL: Nesting depth ${telemetry.maxDepth} exceeds threshold ${thresholds("max_depth")}"
      }

      val alertList = alerts.result()
      if (alertList.nonEmpty) {
        println(s"\n⚠️ ALERTS TRIGGERED:")
        alertList.foreach(println)
      } else {
        println(s"\n✅ All metrics within thresholds")
      }
    }

    // ===== Phase 5: Schema Drift Detection =====
    println("\n" + "=" * 60)
    println("PHASE 5: SCHEMA DRIFT DETECTION")
    println("=" * 60)

    println("\nSimulating schema evolution over time...")

    val originalSchema = tables("production.user_events").schema
    val originalAlignment = ToonAlignmentAnalyzer.analyzeSchema(originalSchema)

    println(s"\n[Baseline Schema]")
    println(s"  Alignment score: ${originalAlignment.score}")
    println(s"  Max depth: ${originalAlignment.maxDepth}")
    println(s"  Schema hash: ${computeSchemaHash(originalSchema)}")

    // Simulate schema evolution (add nested field)
    val evolvedDf = tables("production.user_events")
      .withColumn("metadata", struct(col("name"), col("age"))) // Add nesting

    val evolvedAlignment = ToonAlignmentAnalyzer.analyzeSchema(evolvedDf.schema)

    println(s"\n[Evolved Schema]")
    println(s"  Alignment score: ${evolvedAlignment.score}")
    println(s"  Max depth: ${evolvedAlignment.maxDepth}")
    println(s"  Schema hash: ${computeSchemaHash(evolvedDf.schema)}")

    val scoreDrift = math.abs(evolvedAlignment.score - originalAlignment.score)
    if (scoreDrift > 0.1) {
      println(f"\n🚨 SCHEMA DRIFT ALERT: Alignment score changed by $scoreDrift%.2f")
      println(s"   Original: ${originalAlignment.score}, Evolved: ${evolvedAlignment.score}")
      println(s"   Action: Re-validate TOON encoding effectiveness")
    }

    // ===== Summary Dashboard =====
    println("\n" + "=" * 60)
    println("PRODUCTION MONITORING DASHBOARD")
    println("=" * 60)

    tables.foreach { case (tableName, df) =>
      val health = assessDataFrameHealth(df, tableName)
      val telemetry = collectTelemetry(df, tableName)

      val status = if (health.productionReady) "✅ HEALTHY" else "❌ UNHEALTHY"

      println(f"""
                 |$tableName: $status
                 |  Alignment: ${telemetry.alignmentScore}%.2f | Rows: ${telemetry.rowCount}%,d | Depth: ${telemetry.maxDepth}
                 |  TOON recommended: ${telemetry.useToon} | Chunk size: ${telemetry.recommendedChunkSize}
                 |""".stripMargin)
    }

    println("\n✅ Monitoring demo complete!")
    println("\nNext steps:")
    println("  1. Review production readiness reports in /tmp")
    println("  2. Configure monitoring system (Datadog, Prometheus, CloudWatch)")
    println("  3. Set up alerting based on thresholds")
    println("  4. Deploy TOON encoding to approved tables only")
  }

  // ===== Mock Data Generators =====

  private def createMockUserEvents()(implicit spark: SparkSession) = {
    import spark.implicits._
    Seq(
      (1, "Alice", 25, "user_signup"),
      (2, "Bob", 30, "purchase"),
      (3, "Charlie", 35, "page_view"),
    ).toDF("user_id", "name", "age", "event_type")
  }

  private def createMockNestedOrders()(implicit spark: SparkSession) = {
    import spark.implicits._
    import org.apache.spark.sql.functions._

    Seq(
      (1, "Order-001", 99.99),
      (2, "Order-002", 149.99),
    ).toDF("order_id", "order_number", "total")
      .withColumn("items", array(struct(lit("item1").as("name"), lit(1).as("quantity"))))
  }

  private def createMockDeepHierarchy()(implicit spark: SparkSession) = {
    import spark.implicits._
    import org.apache.spark.sql.functions._

    Seq(
      (1, "Company A"),
      (2, "Company B"),
    ).toDF("company_id", "name")
      .withColumn("department", struct(
        lit("Engineering").as("name"),
        struct(
          lit("Team A").as("team_name"),
          struct(
            lit("Alice").as("lead_name"),
            array(lit("Bob"), lit("Charlie")).as("members")
          ).as("team_lead")
        ).as("teams")
      ))
  }

  // ===== Monitoring System Integration =====

  private def sendToDatadog(telemetry: TelemetrySnapshot): Unit = {
    // Example Datadog integration
    println(s"  → Datadog: gauge toon.alignment_score=${telemetry.alignmentScore} tags=[table:${telemetry.tableName}]")
    println(s"  → Datadog: gauge toon.row_count=${telemetry.rowCount} tags=[table:${telemetry.tableName}]")
    println(s"  → Datadog: gauge toon.max_depth=${telemetry.maxDepth} tags=[table:${telemetry.tableName}]")
  }

  private def sendToPrometheus(telemetry: TelemetrySnapshot): Unit = {
    // Example Prometheus integration
    println(s"  → Prometheus: toon_alignment_score{table=\"${telemetry.tableName}\"} ${telemetry.alignmentScore}")
    println(s"  → Prometheus: toon_row_count{table=\"${telemetry.tableName}\"} ${telemetry.rowCount}")
  }

  private def formatBytes(bytes: Long): String = {
    if (bytes < 1024) s"$bytes bytes"
    else if (bytes < 1024 * 1024) f"${bytes / 1024.0}%.1f KB"
    else if (bytes < 1024 * 1024 * 1024) f"${bytes / (1024.0 * 1024.0)}%.1f MB"
    else f"${bytes / (1024.0 * 1024.0 * 1024.0)}%.1f GB"
  }

  private def computeSchemaHash(schema: org.apache.spark.sql.types.StructType): String = {
    val schemaStr = schema.fields.map(f => s"${f.name}:${f.dataType.typeName}").mkString(",")
    val hash = java.security.MessageDigest
      .getInstance("MD5")
      .digest(schemaStr.getBytes("UTF-8"))
    hash.map("%02x".format(_)).mkString.take(8)
  }

}
