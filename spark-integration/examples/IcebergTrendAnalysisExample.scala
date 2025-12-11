package examples

import io.toonformat.toon4s.spark.integrations.IcebergTimeTravel._
import io.toonformat.toon4s.spark.monitoring.ToonMonitoring._
import org.apache.spark.sql.SparkSession

import java.time.Instant

/**
 * Iceberg time travel example: Quarterly business trend analysis using LLM.
 *
 * ==Use Case==
 * E-commerce company wants quarterly trend analysis:
 *   - Compare customer behavior across Q1, Q2, Q3, Q4
 *   - Identify seasonal patterns using LLM
 *   - Generate executive summary report
 *
 * ==Why Iceberg + TOON?==
 *   - Iceberg time travel: Efficient historical queries (metadata-only)
 *   - TOON encoding: 22% token savings for tabular snapshots
 *   - Multi-snapshot comparison: Send Q1-Q4 data in single LLM prompt
 *
 * ==Running==
 * {{{
 * // Iceberg table setup (Spark 3.5 + Iceberg 1.4)
 * %sql
 * CREATE TABLE customer_metrics (
 *   customer_id STRING,
 *   total_orders INT,
 *   total_spend DOUBLE,
 *   avg_order_value DOUBLE,
 *   last_purchase_date DATE
 * ) USING ICEBERG;
 *
 * spark-submit --class examples.IcebergTrendAnalysisExample \
 *   --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.4.0 \
 *   toon4s-spark-assembly.jar
 * }}}
 */
object IcebergTrendAnalysisExample {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("TOON Iceberg Trend Analysis")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hive")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .getOrCreate()

    println("=== TOON Iceberg Quarterly Trend Analysis ===")

    val tableName = "spark_catalog.default.customer_metrics"

    // Define quarterly end timestamps
    val q1End = Instant.parse("2024-03-31T23:59:59Z")
    val q2End = Instant.parse("2024-06-30T23:59:59Z")
    val q3End = Instant.parse("2024-09-30T23:59:59Z")
    val q4End = Instant.parse("2024-12-31T23:59:59Z")

    // Step 1: Validate schema consistency across time range
    println("\n[1] Validating schema consistency across quarters...")
    val timestamps = Vector(q1End, q2End, q3End, q4End)
    val alignments = validateSchemaConsistency(tableName, timestamps)

    alignments.foreach { case (ts, alignment) =>
      println(s"  $ts: aligned=${alignment.aligned}, score=${alignment.score}")
    }

    val allAligned = alignments.values.forall(_.aligned)
    if (!allAligned) {
      println("❌ Schema evolution detected! Some quarters not TOON-aligned.")
      println("⚠️ Consider using consistent schema projection.")
      sys.exit(1)
    }

    println("✅ All quarters have consistent TOON-aligned schema")

    // Step 2: Read quarterly snapshots
    println("\n[2] Reading quarterly snapshots...")
    val quarters = Map(
      "Q1" -> q1End,
      "Q2" -> q2End,
      "Q3" -> q3End,
      "Q4" -> q4End,
    )

    val quarterlyData = quarters.map { case (qName, timestamp) =>
      println(s"  Reading $qName 2024 snapshot...")
      val result = readSnapshotWithMetadata(
        timestamp,
        TimeTravelConfig(
          tableName = tableName,
          key = s"customer_metrics_$qName",
          filterPredicate = Some("total_orders > 0"), // Active customers only
        ),
      )

      result match {
        case Right(metadata) =>
          println(s"    ✅ $qName: ${metadata.rowCount} customers, ${metadata.toonChunks.size} chunks")
          (qName, metadata)
        case Left(error) =>
          println(s"    ❌ $qName: Encoding failed - ${error.getMessage}")
          sys.exit(1)
      }
    }

    // Step 3: Generate production readiness report for Q4
    println("\n[3] Generating production readiness report...")
    val q4Snapshot = spark.read
      .format("iceberg")
      .option("as-of-timestamp", formatInstant(q4End))
      .load(tableName)

    val report = generateProductionReport(q4Snapshot, tableName)
    println(report)

    // Save report
    val reportPath = "/tmp/toon-iceberg-readiness-report.md"
    import java.nio.file.{Files, Paths}
    Files.write(Paths.get(reportPath), report.getBytes)
    println(s"\n✅ Report saved to: $reportPath")

    // Step 4: Construct multi-quarter LLM prompt
    println("\n[4] Constructing quarterly comparison prompt...")
    val prompt = buildQuarterlyComparisonPrompt(quarterlyData)

    println(s"Prompt size: ${prompt.length} chars (~${prompt.length / 4} tokens)")

    // Step 5: Send to LLM for trend analysis
    println("\n[5] Sending to LLM for trend analysis...")

    // In production, replace with actual LLM client
    val trendAnalysis = analyzeTrends(prompt)
    println("\n=== LLM Trend Analysis ===")
    println(trendAnalysis)

    // Step 6: Compare Q3 vs Q4 for recent trends
    println("\n[6] Detailed Q3 vs Q4 comparison...")
    val comparison = compareSnapshots(
      tableName = tableName,
      beforeTimestamp = q3End,
      afterTimestamp = q4End,
      config = TimeTravelConfig(
        tableName = tableName,
        filterPredicate = Some("total_orders >= 5"), // High-value customers
      ),
    )

    comparison match {
      case Right((q3Data, q4Data)) =>
        println(s"✅ Q3 data: ${q3Data.size} chunks")
        println(s"✅ Q4 data: ${q4Data.size} chunks")

        val detailedPrompt =
          s"""
             |Analyze high-value customer behavior changes from Q3 to Q4 2024:
             |
             |Q3 2024 Data (Sept 30):
             |${q3Data.mkString("\n")}
             |
             |Q4 2024 Data (Dec 31):
             |${q4Data.mkString("\n")}
             |
             |Focus on:
             |1. Customer retention (did high-value customers continue purchasing?)
             |2. Average order value trends
             |3. Seasonal patterns
             |4. Churn indicators
             |
             |Provide actionable insights for Q1 2025 strategy.
             |""".stripMargin

        val insights = analyzeTrends(detailedPrompt)
        println("\n=== Q3 vs Q4 Insights ===")
        println(insights)

      case Left(error) =>
        println(s"❌ Comparison failed: ${error.getMessage}")
    }

    // Step 7: Generate weekly time series for Q4
    println("\n[7] Generating weekly time series for Q4...")
    val q4Start = Instant.parse("2024-10-01T00:00:00Z")
    val timeSeries = generateSnapshotTimeSeries(
      tableName = tableName,
      startTime = q4Start,
      endTime = q4End,
      intervalSeconds = 7 * 24 * 3600, // Weekly
      config = TimeTravelConfig(tableName = tableName),
    )

    timeSeries match {
      case Right(snapshots) =>
        println(s"✅ Generated ${snapshots.size} weekly snapshots")
        snapshots.foreach { case (timestamp, toonChunks) =>
          println(s"  Week of $timestamp: ${toonChunks.size} chunks")
        }

        // Analyze weekly momentum
        val weeklyPrompt = buildWeeklyMomentumPrompt(snapshots)
        val momentum = analyzeTrends(weeklyPrompt)
        println("\n=== Weekly Momentum Analysis ===")
        println(momentum)

      case Left(error) =>
        println(s"❌ Time series generation failed: ${error.getMessage}")
    }

    println("\n✅ Analysis complete!")
  }

  /**
   * Build multi-quarter comparison prompt.
   */
  private def buildQuarterlyComparisonPrompt(
      quarterlyData: Map[String, SnapshotMetadata]
  ): String = {
    val sb = new StringBuilder()

    sb.append("Analyze customer metrics trends across 2024 quarters:\n\n")

    quarterlyData.toSeq.sortBy(_._1).foreach { case (qName, metadata) =>
      sb.append(s"=== $qName 2024 ($qName end: ${metadata.timestamp}) ===\n")
      sb.append(s"Active customers: ${metadata.rowCount}\n")
      sb.append(s"Data:\n")
      metadata.toonChunks.foreach { chunk =>
        sb.append(chunk)
        sb.append("\n")
      }
      sb.append("\n")
    }

    sb.append(
      """
        |Instructions:
        |1. Identify key trends across quarters (growth, churn, spending patterns)
        |2. Compare Q1 (winter) vs Q2 (spring) vs Q3 (summer) vs Q4 (holiday season)
        |3. Highlight seasonal patterns
        |4. Recommend Q1 2025 strategy based on annual trends
        |
        |Format response as executive summary with key metrics.
        |""".stripMargin
    )

    sb.result()
  }

  /**
   * Build weekly momentum prompt.
   */
  private def buildWeeklyMomentumPrompt(
      snapshots: Vector[(Instant, Vector[String])]
  ): String = {
    val sb = new StringBuilder()

    sb.append("Analyze week-over-week momentum in Q4 2024:\n\n")

    snapshots.foreach { case (timestamp, toonChunks) =>
      sb.append(s"Week of $timestamp:\n")
      toonChunks.foreach { chunk =>
        sb.append(chunk)
        sb.append("\n")
      }
      sb.append("\n")
    }

    sb.append(
      """
        |Instructions:
        |1. Identify weekly growth/decline trends
        |2. Detect holiday season impact (Black Friday, Christmas)
        |3. Predict momentum for early Q1 2025
        |""".stripMargin
    )

    sb.result()
  }

  /**
   * Simulate LLM trend analysis.
   *
   * In production, replace with actual LLM client.
   */
  private def analyzeTrends(prompt: String): String = {
    // In real implementation:
    //   val conversation = Conversation.fromPrompts(
    //     "You are a business intelligence analyst specializing in e-commerce trends.",
    //     prompt
    //   )
    //   val response = llmClient.complete(conversation)
    //   response.content

    // Mock analysis for demo
    """
      |EXECUTIVE SUMMARY: 2024 Customer Trends
      |
      |KEY FINDINGS:
      |1. Annual Growth: +15% active customers (Q4 vs Q1)
      |2. Holiday Surge: Q4 shows 40% increase in avg order value
      |3. Retention Strong: 82% of Q1 customers still active in Q4
      |4. Seasonal Pattern: Clear dip in Q3 (summer), recovery in Q4
      |
      |Q1 2025 RECOMMENDATIONS:
      |- Leverage holiday momentum with post-season promotions
      |- Re-engage Q3 churned customers before spring
      |- Focus on high-value segment (grew 25% in Q4)
      |""".stripMargin
  }

  /**
   * Format Instant for Iceberg time travel.
   */
  private def formatInstant(instant: Instant): String = {
    val ldt = java.time.LocalDateTime.ofInstant(instant, java.time.ZoneId.of("UTC"))
    ldt.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"))
  }

}
