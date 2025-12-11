package examples

import io.toonformat.toon4s.spark.SparkToonOps._
import io.toonformat.toon4s.spark.integrations.DeltaLakeCDC._
import io.toonformat.toon4s.spark.monitoring.ToonMonitoring._
import org.apache.spark.sql.SparkSession

/**
 * Databricks streaming example: Real-time fraud detection using TOON-encoded CDC events.
 *
 * ==Use Case==
 * Financial institution monitors transaction table for fraud patterns using LLM analysis.
 * Requirements:
 *   - Real-time processing (< 30 second latency)
 *   - Cost-efficient LLM API usage (TOON saves 22% tokens)
 *   - Schema alignment validation (prevent encoding failures)
 *   - Production monitoring (health checks, telemetry)
 *
 * ==Architecture==
 * 1. Delta Lake table with Change Data Feed enabled
 * 2. Structured Streaming reads CDC events
 * 3. TOON encoding with adaptive chunking
 * 4. LLM analyzes TOON-encoded micro-batches
 * 5. Write fraud alerts back to Delta Lake
 *
 * ==Running on Databricks==
 * {{{
 * // Create Delta table with CDC
 * %sql
 * CREATE TABLE transactions (
 *   txn_id STRING,
 *   user_id STRING,
 *   amount DOUBLE,
 *   merchant STRING,
 *   timestamp TIMESTAMP
 * ) USING DELTA
 * TBLPROPERTIES (delta.enableChangeDataFeed = true);
 *
 * // Run streaming job
 * spark-submit --class examples.DatabricksStreamingExample \
 *   --master yarn \
 *   --deploy-mode cluster \
 *   toon4s-spark-assembly.jar
 * }}}
 */
object DatabricksStreamingExample {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("TOON Fraud Detection Stream")
      .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
      .getOrCreate()

    import spark.implicits._

    println("=== TOON Fraud Detection Stream ===")

    // Step 1: Validate table schema is TOON-aligned
    println("\n[1] Validating schema alignment...")
    val alignment = validateTableAlignment("transactions")

    if (!alignment.aligned) {
      println(s"❌ Schema not TOON-aligned: ${alignment.recommendation}")
      println("⚠️ This will cause encoding failures in production!")
      alignment.warnings.foreach(w => println(s"  - $w"))
      sys.exit(1)
    }

    println(s"✅ Schema aligned (score: ${alignment.score})")
    println(s"   Expected accuracy: ${alignment.expectedAccuracy}")

    // Step 2: Configure CDC streaming
    println("\n[2] Configuring CDC stream...")
    val config = DeltaCDCConfig(
      tableName = "transactions",
      checkpointLocation = "/dbfs/checkpoints/fraud-detection",
      triggerInterval = "30 seconds",
      maxFilesPerTrigger = Some(100), // Rate limiting
      key = "transactions",
    )

    // Step 3: Start streaming with monitoring
    println("\n[3] Starting TOON CDC stream...")
    val query = streamDeltaCDCWithMetadata(config) { metadata =>
      println(s"\n--- Batch ${metadata.batchId} ---")
      println(s"Commit versions: ${metadata.commitVersion._1} - ${metadata.commitVersion._2}")
      println(s"Change types: ${metadata.changeTypes}")
      println(s"TOON chunks: ${metadata.toonChunks.size}")
      println(s"Alignment score: ${metadata.alignmentScore}")

      // Step 4: Filter for suspicious transactions (inserts/updates only)
      val suspiciousChanges = metadata.changeTypes.filterKeys(k => k == "insert" || k.startsWith("update"))

      if (suspiciousChanges.values.sum > 0) {
        println(s"Processing ${suspiciousChanges.values.sum} suspicious changes...")

        // Step 5: Send to LLM for fraud analysis
        metadata.toonChunks.zipWithIndex.foreach { case (toon, idx) =>
          println(s"\n[LLM Analysis] Chunk $idx")

          // In production, replace with actual LLM client
          val prompt =
            s"""
               |Analyze these transactions for fraud patterns:
               |
               |$toon
               |
               |Identify:
               |1. Unusual spending patterns
               |2. Geographic anomalies
               |3. Velocity attacks
               |4. Account takeover indicators
               |
               |Return JSON with fraud alerts.
               |""".stripMargin

          // Simulate LLM call
          val fraudAlerts = analyzeFraudPatterns(prompt)

          // Step 6: Write alerts to Delta Lake
          if (fraudAlerts.nonEmpty) {
            val alertsDF = fraudAlerts.toDF("txn_id", "fraud_type", "confidence", "timestamp")
            alertsDF.write
              .format("delta")
              .mode("append")
              .option("mergeSchema", "true")
              .saveAsTable("fraud_alerts")

            println(s"✅ Written ${fraudAlerts.size} fraud alerts")
          }
        }
      }

      // Step 7: Collect telemetry
      val telemetry = TelemetrySnapshot(
        timestamp = java.time.Instant.now(),
        tableName = "transactions",
        rowCount = metadata.cdcEvents.count(),
        columnCount = metadata.cdcEvents.schema.fields.length,
        alignmentScore = metadata.alignmentScore,
        maxDepth = 1, // From schema validation
        estimatedDataSize = metadata.toonChunks.map(_.length.toLong).sum,
        recommendedChunkSize = 1000,
        useToon = true,
        schemaHash = "abcd1234",
      )

      // Send to monitoring system
      sendTelemetry(telemetry)
    }

    // Step 8: Await termination
    println("\n✅ Stream started successfully")
    println("Monitoring at: https://your-databricks-workspace/sql/dashboards/fraud-detection")

    query.awaitTermination()
  }

  /**
   * Simulate LLM fraud analysis.
   *
   * In production, replace with actual LLM client (OpenAI, Anthropic, etc.)
   */
  private def analyzeFraudPatterns(toonData: String): Seq[(String, String, Double, String)] = {
    // Simulate LLM analysis
    // In real implementation:
    //   val response = llmClient.complete(Conversation.fromPrompts("You are fraud analyst", toonData))
    //   parseJsonResponse(response)

    // Mock fraud alerts for demo
    Seq(
      ("txn_001", "velocity_attack", 0.95, java.time.Instant.now().toString),
      ("txn_042", "geographic_anomaly", 0.87, java.time.Instant.now().toString),
    )
  }

  /**
   * Send telemetry to monitoring system.
   *
   * In production, integrate with Datadog, Prometheus, CloudWatch, etc.
   */
  private def sendTelemetry(telemetry: TelemetrySnapshot): Unit = {
    // Example: Datadog
    // statsd.gauge("toon.alignment_score", telemetry.alignmentScore,
    //   tags = Seq(s"table:${telemetry.tableName}"))

    // Example: CloudWatch
    // cloudwatch.putMetric("ToonAlignmentScore", telemetry.alignmentScore,
    //   dimensions = Map("TableName" -> telemetry.tableName))

    println(s"[Telemetry] Alignment: ${telemetry.alignmentScore}, Rows: ${telemetry.rowCount}")
  }

}
