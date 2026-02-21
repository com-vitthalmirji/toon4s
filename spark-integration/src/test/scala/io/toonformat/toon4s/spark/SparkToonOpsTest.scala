package io.toonformat.toon4s.spark

import java.nio.file.Files

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

import io.toonformat.toon4s.EncodeOptions
import io.toonformat.toon4s.spark.SparkToonOps._
import io.toonformat.toon4s.spark.llm.{
  LlmChunkRequest,
  LlmPartitionWriteOptions,
  LlmPartitionWriter,
  LlmPartitionWriterFactory,
  LlmSendStatus,
}
import io.toonformat.toon4s.spark.testkit.SparkTestSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.llm4s.error.{LLMError, NetworkError, TimeoutError, ValidationError}

class SparkToonOpsTest extends SparkTestSuite {

  private val isWindows = System.getProperty("os.name", "").toLowerCase.contains("win")

  test("toToon: encode simple DataFrame") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("age", IntegerType),
    ))

    val data = Seq(
      Row(1, "Alice", 25),
      Row(2, "Bob", 30),
    )

    val df = spark.createDataFrame(data.asJava, schema)
    val result = df.toToon(key = "users", maxRowsPerChunk = 100)

    assert(result.isRight)
    result.foreach { chunks =>
      assertEquals(chunks.size, 1)
      assert(chunks.head.contains("users"))
      assert(chunks.head.contains("Alice"))
      assert(chunks.head.contains("Bob"))
    }
  }

  test("toToon: encode with ToonSparkOptions") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
    ))
    val data = Seq(
      Row(1, "Alice"),
      Row(2, "Bob"),
      Row(3, "Cara"),
    )
    val df = spark.createDataFrame(data.asJava, schema)
    val options = ToonSparkOptions(
      key = "users",
      maxRowsPerChunk = 2,
      encodeOptions = EncodeOptions(),
    )

    val result = df.toToon(options)

    assert(result.isRight)
    result.foreach { chunks =>
      assertEquals(chunks.size, 2)
      assert(chunks.head.contains("users"))
    }
  }

  test("toToon: handle empty DataFrame") {
    val df = spark.emptyDataFrame

    val result = df.toToon()

    assert(result.isRight)
    result.foreach(chunks => assertEquals(chunks.size, 1))
  }

  test("writeToon: writes TOON chunks without collecting on driver") {
    assume(!isWindows, "writeToon test needs winutils on Windows CI")
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
    ))
    val data = (1 to 12).map(i => Row(i, s"user$i"))
    val df = spark.createDataFrame(data.asJava, schema).repartition(3)
    val tempDir = Files.createTempDirectory("toon-write-test")
    val outputDir = tempDir.resolve("chunks-output")
    val outputPath =
      if (System.getProperty("os.name", "").toLowerCase.contains("win")) outputDir.toUri.toString
      else outputDir.toAbsolutePath.toString

    val writeResult = df.writeToon(
      outputPath = outputPath,
      key = "users",
      maxRowsPerChunk = 4,
      options = EncodeOptions(),
    )

    assert(writeResult.isRight, clues(writeResult.left.toOption.map(_.message).getOrElse("")))

    val loaded = spark.read.text(outputPath)
    val s = spark
    import s.implicits._
    assert(loaded.count() > 0L)
    assert(loaded.as[String].take(100).exists(_.contains("users")))
  }

  test("writeToLlmPartitions: send from partition tasks and aggregate metrics") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
    ))
    val data = (1 to 24).map(i => Row(i, s"user$i"))
    val df = spark.createDataFrame(data.asJava, schema).repartition(3)

    val factory = new LlmPartitionWriterFactory {
      def create(): LlmPartitionWriter = new LlmPartitionWriter {
        def send(request: LlmChunkRequest): Either[LLMError, LlmSendStatus] = {
          Right(LlmSendStatus.Sent)
        }
      }
    }

    val result = df.writeToLlmPartitions(
      writerFactory = factory,
      llmOptions = LlmPartitionWriteOptions(
        maxRetries = 1,
        retryBackoffMs = 1L,
        failOnError = true,
        idempotencyPrefix = "test-prefix",
      ),
      options = ToonSparkOptions(
        key = "users",
        maxRowsPerChunk = 4,
        encodeOptions = EncodeOptions(),
      ),
    )

    assert(result.isRight)
    result.foreach { metrics =>
      assert(metrics.partitionCount >= 1L)
      assert(metrics.attemptedChunks > 0L)
      assertEquals(metrics.attemptedChunks, metrics.sentChunks)
      assertEquals(metrics.failedChunks, 0L)
      assertEquals(metrics.duplicateChunks, 0L)
      assert(metrics.totalBytes > 0L)
      assert(metrics.totalEstimatedTokens > 0L)
      assert(metrics.firstError.isEmpty)
    }
  }

  test("writeToLlmPartitions: include retry-safe idempotency keys") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
    ))
    val data = (1 to 8).map(i => Row(i, s"user$i"))
    val df = spark.createDataFrame(data.asJava, schema).repartition(2)

    val factory = new LlmPartitionWriterFactory {
      def create(): LlmPartitionWriter = new LlmPartitionWriter {
        def send(request: LlmChunkRequest): Either[LLMError, LlmSendStatus] = {
          if (request.idempotencyKey.startsWith("idem-check:users:p")) Right(LlmSendStatus.Sent)
          else Left(ValidationError("idempotencyKey", "bad idempotency key"))
        }
      }
    }

    val result = df.writeToLlmPartitions(
      writerFactory = factory,
      llmOptions = LlmPartitionWriteOptions(
        maxRetries = 0,
        retryBackoffMs = 0L,
        failOnError = true,
        idempotencyPrefix = "idem-check",
      ),
      options = ToonSparkOptions("users", maxRowsPerChunk = 2, EncodeOptions()),
    )

    assert(result.isRight)
  }

  test("writeToLlmPartitions: retries recoverable failures per chunk") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
    ))
    val data = (1 to 10).map(i => Row(i, s"user$i"))
    val df = spark.createDataFrame(data.asJava, schema).repartition(2)

    val factory = new LlmPartitionWriterFactory {
      def create(): LlmPartitionWriter = new LlmPartitionWriter {
        private val attemptsByKey = scala.collection.mutable.Map.empty[String, Int]

        def send(request: LlmChunkRequest): Either[LLMError, LlmSendStatus] = {
          val attempt = attemptsByKey.getOrElse(request.idempotencyKey, 0) + 1
          attemptsByKey.put(request.idempotencyKey, attempt)
          if (attempt == 1) Left(TimeoutError("transient timeout", 50.millis, "llm-send"))
          else Right(LlmSendStatus.Sent)
        }
      }
    }

    val result = df.writeToLlmPartitions(
      writerFactory = factory,
      llmOptions = LlmPartitionWriteOptions(
        maxRetries = 1,
        retryBackoffMs = 1L,
        failOnError = true,
        idempotencyPrefix = "retry-check",
      ),
      options = ToonSparkOptions("users", maxRowsPerChunk = 2, EncodeOptions()),
    )

    assert(result.isRight)
    result.foreach { metrics =>
      assert(metrics.attemptedChunks > 0L)
      assertEquals(metrics.failedChunks, 0L)
      assertEquals(metrics.sentChunks, metrics.attemptedChunks)
    }
  }

  test("writeToLlmPartitions: failOnError returns Left for permanent failures") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
    ))
    val data = (1 to 5).map(i => Row(i, s"user$i"))
    val df = spark.createDataFrame(data.asJava, schema)

    val factory = new LlmPartitionWriterFactory {
      def create(): LlmPartitionWriter = new LlmPartitionWriter {
        def send(request: LlmChunkRequest): Either[LLMError, LlmSendStatus] =
          Left(NetworkError("permanent network failure", None, "llm-endpoint"))
      }
    }

    val result = df.writeToLlmPartitions(
      writerFactory = factory,
      llmOptions = LlmPartitionWriteOptions(
        maxRetries = 0,
        retryBackoffMs = 0L,
        failOnError = true,
      ),
      options = ToonSparkOptions("users", maxRowsPerChunk = 2, EncodeOptions()),
    )

    assert(result.isLeft)
    result.left.foreach(error =>
      assert(error.message.contains("Failed to write TOON chunks to LLM partitions"))
    )
  }

  test("toToon: chunk large DataFrame") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
    ))

    val data = (1 to 250).map(i => Row(i, s"user$i"))
    val df = spark.createDataFrame(data.asJava, schema)

    val result = df.toToon(maxRowsPerChunk = 100)

    assert(result.isRight)
    result.foreach { chunks =>
      assertEquals(chunks.size, 3) // 100 + 100 + 50
    }
  }

  test("toToon: reject non-positive chunk size") {
    val schema = StructType(Seq(
      StructField("id", IntegerType)
    ))
    val df = spark.createDataFrame(Seq(Row(1)).asJava, schema)

    val result = df.toToon(maxRowsPerChunk = 0)
    assert(result.isLeft)
  }

  test("toToon: respect max collected chunk safety limit") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
    ))
    val data = (1 to 5).map(i => Row(i, s"user$i"))
    val df = spark.createDataFrame(data.asJava, schema)

    spark.conf.set("toon4s.spark.collect.maxChunks", "1")
    try {
      val result = df.toToon(maxRowsPerChunk = 2)
      assert(result.isLeft)
      result.left.foreach(error =>
        assert(error.message.contains("toToonDataset"))
      )
    } finally {
      spark.conf.unset("toon4s.spark.collect.maxChunks")
    }
  }

  test("toToon: respect max collected payload safety limit") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
    ))
    val data = Seq(Row(1, "Alice"))
    val df = spark.createDataFrame(data.asJava, schema)

    spark.conf.set("toon4s.spark.collect.maxPayloadBytes", "8")
    try {
      val result = df.toToon(maxRowsPerChunk = 10)
      assert(result.isLeft)
      result.left.foreach(error =>
        assert(error.message.contains("payload size"))
      )
    } finally {
      spark.conf.unset("toon4s.spark.collect.maxPayloadBytes")
    }
  }

  test("toonMetrics: compute token metrics") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
    ))

    val data = Seq(
      Row(1, "Alice"),
      Row(2, "Bob"),
    )

    val df = spark.createDataFrame(data.asJava, schema)
    val result = df.toonMetrics(key = "data")

    assert(result.isRight)
    result.foreach { metrics =>
      assert(metrics.jsonTokenCount > 0)
      assert(metrics.toonTokenCount > 0)
      assert(metrics.rowCount == 2)
      assert(metrics.columnCount == 2)
    }
  }

  test("toonMetrics: compute consistent token accounting") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("score", DoubleType),
    ))

    val data = Seq(
      Row(1, "value1", 100.0),
      Row(2, "value2", 200.0),
      Row(3, "value3", 300.0),
    )

    val df = spark.createDataFrame(data.asJava, schema)
    val result = df.toonMetrics()

    assert(result.isRight)
    result.foreach { metrics =>
      assert(metrics.jsonTokenCount > 0)
      assert(metrics.toonTokenCount > 0)
      assertEquals(
        metrics.absoluteSavings,
        metrics.jsonTokenCount - metrics.toonTokenCount,
      )
    }
  }

  test("toonMetrics: support caller-provided chunk size") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
    ))
    val data = (1 to 25).map(i => Row(i, s"user$i"))
    val df = spark.createDataFrame(data.asJava, schema)

    val result = df.toonMetrics("data", maxRowsPerChunk = 3, EncodeOptions())
    assert(result.isRight)
    result.foreach { metrics =>
      assertEquals(metrics.rowCount, 25)
      assertEquals(metrics.columnCount, 2)
    }
  }

  test("toonMetrics: support ToonSparkOptions") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
    ))
    val data = (1 to 11).map(i => Row(i, s"user$i"))
    val df = spark.createDataFrame(data.asJava, schema)

    val result = df.toonMetrics(
      ToonSparkOptions(
        key = "users",
        maxRowsPerChunk = 4,
        encodeOptions = EncodeOptions(),
      )
    )

    assert(result.isRight)
    result.foreach { metrics =>
      assertEquals(metrics.rowCount, 11)
      assertEquals(metrics.columnCount, 2)
      assert(metrics.jsonTokenCount > 0)
      assert(metrics.toonTokenCount > 0)
    }
  }

  test("toonMetricsWithEstimator: support custom estimator") {
    val rows = Seq(
      org.apache.spark.sql.Row(1, "Alice"),
      org.apache.spark.sql.Row(2, "Bob"),
      org.apache.spark.sql.Row(3, "Charlie"),
    )
    val schema = StructType(
      Seq(StructField("id", IntegerType), StructField("name", StringType))
    )
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(rows),
      schema,
    )
    val estimator = ToonMetrics.CharsPerTokenEstimator(charsPerToken = 2.0)

    val result = df.toonMetricsWithEstimator(
      key = "data",
      options = EncodeOptions(),
      tokenEstimator = estimator,
    )

    assert(result.isRight)
    result.foreach { metrics =>
      assert(metrics.jsonTokenCount > 0)
      assert(metrics.toonTokenCount > 0)
    }
  }

  test("toonMetrics: reject non-positive chunk size") {
    val schema = StructType(Seq(StructField("id", IntegerType)))
    val df = spark.createDataFrame(Seq(Row(1)).asJava, schema)

    val result = df.toonMetrics("data", maxRowsPerChunk = 0, EncodeOptions())
    assert(result.isLeft)
  }

  test("toonMetrics: respect max collected partition metrics safety limit") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
    ))
    val data = (1 to 12).map(i => Row(i, s"user$i"))
    val df = spark.createDataFrame(data.asJava, schema).repartition(3)

    spark.conf.set("toon4s.spark.metrics.maxPartitionStats", "1")
    try {
      val result = df.toonMetrics()
      assert(result.isLeft)
      result.left.foreach(error =>
        assert(error.message.contains("Partition metrics count exceeds safety limit"))
      )
    } finally {
      spark.conf.unset("toon4s.spark.metrics.maxPartitionStats")
    }
  }

  test("toonMetrics: handle empty DataFrame") {
    val df = spark.emptyDataFrame
    val result = df.toonMetrics()

    assert(result.isRight)
    result.foreach { metrics =>
      assertEquals(metrics.rowCount, 0)
      assertEquals(metrics.columnCount, 0)
      assert(metrics.jsonTokenCount >= 0)
      assert(metrics.toonTokenCount >= 0)
    }
  }

  test("fromToon: decode TOON to DataFrame") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("score", DoubleType),
    ))

    val data = Seq(
      Row(1, "Alice", 25.5),
      Row(2, "Bob", 30.0),
    )

    val originalDf = spark.createDataFrame(data.asJava, schema)
    val toonResult = originalDf.toToon(key = "data")

    assert(toonResult.isRight)

    toonResult.foreach { toonChunks =>
      val decodedResult = SparkToonOps.fromToon(toonChunks, schema)(spark)

      assert(decodedResult.isRight)
      decodedResult.foreach { decodedDf =>
        assertEquals(decodedDf.count(), 2L)
        assertEquals(decodedDf.schema, schema)
      }
    }
  }

  test("fromToonDataset: decode distributed TOON dataset to DataFrame") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("score", DoubleType),
    ))

    val data = (1 to 50).map(i => Row(i, s"user$i", i.toDouble))
    val sourceDf = spark.createDataFrame(data.asJava, schema).repartition(4)
    val options = ToonSparkOptions(
      key = "events",
      maxRowsPerChunk = 7,
      encodeOptions = EncodeOptions(),
    )

    val toonDataset = sourceDf.toToonDataset(options)
    val result = SparkToonOps.fromToonDataset(toonDataset, schema)

    assert(result.isRight)
    result.foreach { decodedDf =>
      assertEquals(decodedDf.count(), 50L)
      assertEquals(decodedDf.schema, schema)
    }
  }

  test("fromToon: delegates to dataset decode path") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
    ))
    val sourceRows = Seq(
      Row(1, "Alice"),
      Row(2, "Bob"),
    )
    val sourceDf = spark.createDataFrame(sourceRows.asJava, schema)
    val toonChunksResult = sourceDf.toToon(key = "data")

    assert(toonChunksResult.isRight)
    val toonChunks = toonChunksResult.toOption.getOrElse(Vector.empty)

    val result = SparkToonOps.fromToon(toonChunks, schema)

    assert(result.isRight)
    result.foreach { decodedDf =>
      assertEquals(decodedDf.count(), 2L)
      assertEquals(decodedDf.schema, schema)
    }
  }

  test("round-trip: DataFrame -> TOON -> DataFrame") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("active", BooleanType),
    ))

    val data = Seq(
      Row(1, "Alice", true),
      Row(2, "Bob", false),
      Row(3, "Charlie", true),
    )

    val originalDf = spark.createDataFrame(data.asJava, schema)

    val roundTrip = for {
      toonChunks <- originalDf.toToon(key = "data")
      decodedDf <- SparkToonOps.fromToon(toonChunks, schema)(spark)
    } yield decodedDf

    assert(roundTrip.isRight)
    roundTrip.foreach { decodedDf =>
      assertEquals(decodedDf.count(), 3L)
      assertEquals(decodedDf.schema, originalDf.schema)
    }
  }

  test("toToon: handle complex schema") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("tags", ArrayType(StringType)),
    ))

    val data = Seq(
      Row(1, "Alice", Seq("tag1", "tag2").asJava),
      Row(2, "Bob", Seq("tag3").asJava),
    )

    val df = spark.createDataFrame(data.asJava, schema)
    val result = df.toToon()

    assert(result.isRight)
    result.foreach { chunks =>
      assert(chunks.head.contains("tag1"))
      assert(chunks.head.contains("tag2"))
    }
  }

  test("toonMetrics: handle large DataFrames") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("score", DoubleType),
    ))

    val data = (1 to 1000).map(i => Row(i, s"user$i", i * 10.0))
    val df = spark.createDataFrame(data.asJava, schema)

    val result = df.toonMetrics()

    assert(result.isRight)
    result.foreach { metrics =>
      assertEquals(metrics.rowCount, 1000)
      assert(metrics.jsonTokenCount > 0)
      assert(metrics.toonTokenCount > 0)
      assertEquals(
        metrics.absoluteSavings,
        metrics.jsonTokenCount - metrics.toonTokenCount,
      )
    }
  }

  test("distributed round-trip: 10k rows without driver collect") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("score", DoubleType),
    ))

    val data = (1 to 10000).map(i => Row(i, s"user$i", i.toDouble / 10.0))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data, 4),
      schema,
    )

    val options = ToonSparkOptions(key = "users", maxRowsPerChunk = 500)
    val toonChunks = df.toToonDataset(options)
    val decoded = SparkToonOps.fromToonDataset(toonChunks, schema)

    assert(decoded.isRight)
    decoded.foreach { decodedDf =>
      assertEquals(decodedDf.count(), 10000L)
      assertEquals(decodedDf.schema, schema)
    }
  }

}
