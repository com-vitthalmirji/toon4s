package io.toonformat.toon4s.spark.integrations

import io.toonformat.toon4s.spark.{AdaptiveChunking, ToonAlignmentAnalyzer}
import io.toonformat.toon4s.spark.SparkToonOps._
import io.toonformat.toon4s.spark.error.SparkToonError
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

/**
 * Delta lake Change data feed integration for real-time TOON streaming.
 *
 * ==Use case: Real-time LLM processing pipeline==
 * Databricks workloads often need to process CDC events for:
 *   - Real-time fraud detection (LLM analyzes transaction patterns)
 *   - Customer behavior analysis (LLM identifies trends)
 *   - Data quality monitoring (LLM detects anomalies)
 *
 * ==Problem with JSON streaming==
 * Traditional approach uses JSON for streaming:
 * {{{
 * spark.readStream
 *   .format("delta")
 *   .option("readChangeFeed", "true")
 *   .table("events")
 *   .writeStream
 *   .foreachBatch { (batch, _) =>
 *     val json = batch.toJSON.collect().mkString("\n")
 *     sendToLLM(json) // Token-inefficient, high latency
 *   }
 * }}}
 *
 * ==TOON solution==
 * TOON provides:
 *   - 22% token savings for tabular CDC events (benchmark-proven)
 *   - Adaptive chunking to amortize prompt tax
 *   - Schema alignment detection to prevent failures
 *
 * ==Usage==
 * {{{
 * import io.toonformat.toon4s.spark.integrations.DeltaLakeCDC._
 *
 * // Configure CDC stream
 * val config = DeltaCDCConfig(
 *   tableName = "sales.transactions",
 *   startingVersion = Some(100),
 *   checkpointLocation = "/dbfs/checkpoints/toon-cdc"
 * )
 *
 * // Start real-time TOON streaming
 * val query = streamDeltaCDCToToon(config) { (toonChunks, batchId) =>
 *   // Send to LLM for analysis
 *   toonChunks.foreach { toon =>
 *     llmClient.analyze(toon) match {
 *       case Right(insights) => processInsights(insights)
 *       case Left(error) => logger.error("LLM error: " + error)
 *     }
 *   }
 * }
 *
 * query.awaitTermination()
 * }}}
 *
 * ==CDC Event Structure==
 * Delta Lake CDC events have schema:
 * {{{
 * _change_type: string (insert, update_preimage, update_postimage, delete)
 * _commit_version: long
 * _commit_timestamp: timestamp
 * [original table columns...]
 * }}}
 *
 * ==Schema alignment check==
 * Before streaming, this module validates the schema is TOON-aligned:
 *   - Tabular CDC events (depth 0-2): ✅ TOON wins
 *   - Nested CDC events (depth 3+): ⚠️ Consider JSON
 *
 * @see
 *   [[https://docs.delta.io/latest/delta-change-data-feed.html Delta Lake Change Data Feed]]
 * @see
 *   [[https://docs.databricks.com/structured-streaming/delta-lake.html Databricks Streaming]]
 */
object DeltaLakeCDC {

  /**
   * Configuration for Delta Lake CDC streaming.
   *
   * @param tableName
   *   Fully qualified Delta table name (e.g., "catalog.schema.table")
   * @param startingVersion
   *   Starting Delta table version (None = latest)
   * @param startingTimestamp
   *   Starting timestamp (alternative to startingVersion)
   * @param checkpointLocation
   *   Checkpoint directory for streaming state
   * @param maxFilesPerTrigger
   *   Rate limiting (None = process all available data)
   * @param triggerInterval
   *   Micro-batch interval (e.g., "10 seconds")
   * @param key
   *   TOON key name for encoded chunks
   * @param maxRowsPerChunk
   *   Maximum rows per TOON chunk (None = use adaptive chunking)
   */
  final case class DeltaCDCConfig(
      tableName: String,
      startingVersion: Option[Long] = None,
      startingTimestamp: Option[String] = None,
      checkpointLocation: String,
      maxFilesPerTrigger: Option[Int] = None,
      triggerInterval: String = "10 seconds",
      key: String = "cdc_events",
      maxRowsPerChunk: Option[Int] = None,
  )

  /**
   * CDC batch metadata provided to foreachBatch processor.
   *
   * @param batchId
   *   Unique batch ID
   * @param cdcEvents
   *   Raw CDC DataFrame (before TOON encoding)
   * @param toonChunks
   *   TOON-encoded chunks
   * @param changeTypes
   *   Breakdown of change types (insert, update, delete counts)
   * @param commitVersion
   *   Delta table version range for this batch
   * @param alignmentScore
   *   Schema alignment score (0.0-1.0)
   */
  final case class CDCBatchMetadata(
      batchId: Long,
      cdcEvents: DataFrame,
      toonChunks: Vector[String],
      changeTypes: Map[String, Long],
      commitVersion: (Long, Long),
      alignmentScore: Double,
  )

  /**
   * Stream Delta lake CDC events as TOON-encoded micro-batches.
   *
   * This is the primary API for real-time TOON streaming from Delta Lake.
   *
   * ==Architecture==
   *   1. Read CDC stream with readChangeFeed=true
   *   2. For each micro-batch:
   *      a. Validate schema alignment (warn if not TOON-friendly)
   *      b. Apply adaptive chunking to optimize prompt tax
   *      c. Encode batch to TOON
   *      d. Invoke user callback with metadata
   *
   * ==Error handling==
   * If TOON encoding fails for a batch, this method fails the micro-batch. Structured Streaming
   * retry semantics then decide whether the query can recover.
   *
   * @param config
   *   CDC streaming configuration
   * @param processor
   *   Callback for each TOON-encoded batch
   * @param spark
   *   Spark session (must have Delta Lake enabled)
   * @return
   *   StreamingQuery handle
   *
   * @example
   *   {{{
   * val config = DeltaCDCConfig(
   *   tableName = "production.user_events",
   *   checkpointLocation = "/dbfs/checkpoints/toon-cdc"
   * )
   *
   * val query = streamDeltaCDCToToon(config) { (toonChunks, batchId) =>
   *   toonChunks.foreach(llmClient.analyze)
   * }
   *
   * query.awaitTermination()
   *   }}}
   */
  def streamDeltaCDCToToon(
      config: DeltaCDCConfig
  )(processor: (Vector[String], Long) => Unit)(implicit spark: SparkSession): StreamingQuery = {
    streamDeltaCDCWithMetadata(config)(metadata => processor(metadata.toonChunks, metadata.batchId))
  }

  /**
   * Stream Delta lake CDC events with full batch metadata.
   *
   * Advanced API that provides additional metadata (change type breakdown, alignment score, etc.)
   * for monitoring and debugging.
   *
   * @param config
   *   CDC streaming configuration
   * @param processor
   *   Callback with full batch metadata
   * @param spark
   *   Spark session
   * @return
   *   StreamingQuery handle
   *
   * @example
   *   {{{
   * val config = DeltaCDCConfig(tableName = "events", checkpointLocation = "/checkpoints")
   *
   * val query = streamDeltaCDCWithMetadata(config) { metadata =>
   *   logger.info("Batch " + metadata.batchId + ": " + metadata.changeTypes)
   *   logger.info("Alignment score: " + metadata.alignmentScore)
   *   metadata.toonChunks.foreach(llmClient.analyze)
   * }
   *   }}}
   */
  def streamDeltaCDCWithMetadata(
      config: DeltaCDCConfig
  )(processor: CDCBatchMetadata => Unit)(implicit spark: SparkSession): StreamingQuery = {
    import spark.implicits._

    // Build CDC stream reader
    var reader = spark.readStream
      .format("delta")
      .option("readChangeFeed", "true")

    // Apply starting point
    config.startingVersion.foreach(v => reader = reader.option("startingVersion", v.toString))
    config.startingTimestamp.foreach(ts => reader = reader.option("startingTimestamp", ts))

    // Apply rate limiting
    config.maxFilesPerTrigger.foreach(max =>
      reader = reader.option("maxFilesPerTrigger", max.toString)
    )

    val cdcStream = reader.table(config.tableName)

    // Analyze schema alignment once at startup
    val alignmentScore = ToonAlignmentAnalyzer.analyzeSchema(cdcStream.schema)
    if (!alignmentScore.aligned) {
      spark.sparkContext.setJobDescription(
        s"TOON schema warning: ${alignmentScore.recommendation}"
      )
    }

    // Start streaming with foreachBatch
    cdcStream.writeStream
      .trigger(Trigger.ProcessingTime(config.triggerInterval))
      .option("checkpointLocation", config.checkpointLocation)
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (batchDF.head(1).nonEmpty) {
          processCDCBatch(batchDF, batchId, config, alignmentScore.score, processor)
        }
      }
      .start()
  }

  /**
   * Process a single CDC micro-batch.
   *
   * Internal implementation that:
   *   1. Analyzes change type distribution
   *   2. Applies adaptive chunking
   *   3. Encodes to TOON
   *   4. Invokes user callback
   *
   * @param batchDF
   *   CDC DataFrame for this batch
   * @param batchId
   *   Batch ID
   * @param config
   *   CDC configuration
   * @param alignmentScore
   *   Pre-computed alignment score
   * @param processor
   *   User callback
   */
  private def processCDCBatch(
      batchDF: DataFrame,
      batchId: Long,
      config: DeltaCDCConfig,
      alignmentScore: Double,
      processor: CDCBatchMetadata => Unit,
  ): Unit = {
    import batchDF.sparkSession.implicits._

    // Analyze change types
    val changeTypes = batchDF
      .groupBy("_change_type")
      .count()
      .as[(String, Long)]
      .collect()
      .toMap

    // Get commit version range
    val commitVersions = batchDF
      .select("_commit_version")
      .agg(
        org.apache.spark.sql.functions.min("_commit_version"),
        org.apache.spark.sql.functions.max("_commit_version"),
      )
      .as[(Long, Long)]
      .head()

    // Determine chunk size (adaptive or configured)
    val chunkSize = config.maxRowsPerChunk.getOrElse {
      val strategy = AdaptiveChunking.calculateOptimalChunkSize(batchDF)
      if (!strategy.useToon) {
        // Schema not TOON-friendly, but user explicitly requested TOON streaming
        // Use small chunks to minimize damage
        batchDF.sparkSession.sparkContext.setJobDescription(
          s"Batch $batchId: ${strategy.reasoning}"
        )
      }
      strategy.chunkSize
    }

    // Encode to TOON
    val toonResult = batchDF.toToon(key = config.key, maxRowsPerChunk = chunkSize)

    toonResult match {
    case Right(toonChunks) =>
      val metadata = CDCBatchMetadata(
        batchId = batchId,
        cdcEvents = batchDF,
        toonChunks = toonChunks,
        changeTypes = changeTypes,
        commitVersion = commitVersions,
        alignmentScore = alignmentScore,
      )
      processor(metadata)

    case Left(error: SparkToonError) =>
      // Fail the batch so streaming checkpoint/retry semantics can recover safely.
      batchDF.sparkSession.sparkContext.setJobDescription(
        s"Batch $batchId TOON encoding failed: ${error.message}"
      )
      throw new RuntimeException(s"TOON encoding failed for batch $batchId: ${error.message}")
    }
  }

  /**
   * Read historical CDC events from Delta Lake table.
   *
   * For batch processing or backfilling LLM training data.
   *
   * @param tableName
   *   Delta table name
   * @param startingVersion
   *   Starting version (inclusive)
   * @param endingVersion
   *   Ending version (inclusive)
   * @param key
   *   TOON key name
   * @param maxRowsPerChunk
   *   Chunk size (None = adaptive)
   * @param spark
   *   Spark session
   * @return
   *   Either[SparkToonError, Vector[String]]
   *
   * @example
   *   {{{
   * val historicalToon = readHistoricalCDC(
   *   tableName = "events",
   *   startingVersion = 100,
   *   endingVersion = 200
   * )
   *
   * historicalToon.foreach { chunks =>
   *   chunks.foreach(llmClient.train)
   * }
   *   }}}
   */
  def readHistoricalCDC(
      tableName: String,
      startingVersion: Long,
      endingVersion: Long,
      key: String = "cdc_history",
      maxRowsPerChunk: Option[Int] = None,
  )(implicit spark: SparkSession): Either[SparkToonError, Vector[String]] = {
    val cdcDF = spark.read
      .format("delta")
      .option("readChangeFeed", "true")
      .option("startingVersion", startingVersion.toString)
      .option("endingVersion", endingVersion.toString)
      .table(tableName)

    // Apply adaptive chunking if not specified
    val chunkSize = maxRowsPerChunk.getOrElse {
      AdaptiveChunking.calculateOptimalChunkSize(cdcDF).chunkSize
    }

    cdcDF.toToon(key = key, maxRowsPerChunk = chunkSize)
  }

  /**
   * Validate Delta table schema is TOON-aligned before streaming.
   *
   * Use this during pipeline development to detect non-aligned schemas early.
   *
   * @param tableName
   *   Delta table name
   * @param spark
   *   Spark session
   * @return
   *   AlignmentScore with recommendations
   *
   * @example
   *   {{{
   * val alignment = validateTableAlignment("production.events")
   *
   * if (!alignment.aligned) {
   *   println("Table not TOON-aligned: " + alignment.recommendation)
   *   println("Expected accuracy: " + alignment.expectedAccuracy)
   *   println("Warnings: " + alignment.warnings.mkString("\n"))
   * }
   *   }}}
   */
  def validateTableAlignment(tableName: String)(implicit
      spark: SparkSession): ToonAlignmentAnalyzer.AlignmentScore = {
    val schema = spark.read.format("delta").table(tableName).schema
    ToonAlignmentAnalyzer.analyzeSchema(schema)
  }

}
