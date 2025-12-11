# toon4s-spark

Apache Spark integration for TOON format - encode DataFrames to token-efficient TOON format for LLM processing.

## Features

- **DataFrame ↔ TOON conversion**: Pure functional API with Either error handling
- **Extension methods**: Fluent `.toToon()` and `.toonMetrics()` on DataFrames
- **Token metrics**: Compare JSON vs TOON token counts and cost savings
- **SQL UDFs**: Register TOON functions for use in Spark SQL queries
- **Chunking support**: Handle large DataFrames with automatic chunking
- **LLM client abstraction**: [llm4s](https://github.com/llm4s/llm4s)-compatible conversation-based API
- **Type-safe**: Comprehensive Scala type safety with ADT error handling
- **Forward compatible**: Designed to integrate seamlessly with llm4s when available

## Installation

Add to your `build.sbt`:

```scala
libraryDependencies += "com.vitthalmirji" %% "toon4s-spark" % "0.1.0"
```

For Spark applications, use `Provided` scope since Spark is typically provided by the cluster:

```scala
libraryDependencies ++= Seq(
  "com.vitthalmirji" %% "toon4s-spark" % "0.1.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0" % Provided
)
```

## Quick Start

### DataFrame to TOON

```scala
import io.toonformat.toon4s.spark.SparkToonOps._
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("TOON Example")
  .getOrCreate()

import spark.implicits._

val df = Seq(
  (1, "Alice", 25),
  (2, "Bob", 30)
).toDF("id", "name", "age")

// Encode DataFrame to TOON
df.toToon(key = "users") match {
  case Right(toonChunks) =>
    toonChunks.foreach { toon =>
      println(s"TOON: $toon")
      // Send to LLM or save to storage
    }
  case Left(error) =>
    println(s"Error: ${error.message}")
}
```

### Token Metrics

```scala
// Compare JSON vs TOON token efficiency
df.toonMetrics(key = "data") match {
  case Right(metrics) =>
    println(metrics.summary)
    // Output:
    // Token Metrics:
    //   JSON tokens: 150
    //   TOON tokens: 90
    //   Savings: 60 tokens (40.0%)
    //   Rows: 2, Columns: 3

    val costSavings = metrics.estimatedCostSavings(costPer1kTokens = 0.002)
    println(f"Est. cost savings: $$${costSavings}%.4f")

  case Left(error) =>
    println(s"Error: ${error.message}")
}
```

### SQL UDFs

```scala
import io.toonformat.toon4s.spark.ToonUDFs

// Register TOON functions for SQL
ToonUDFs.register(spark)

spark.sql("""
  SELECT
    id,
    name,
    toon_encode_row(struct(id, name, age)) as toon_data,
    toon_estimate_tokens(struct(id, name, age)) as token_count
  FROM users
""").show()
```

### Round-Trip Conversion

```scala
implicit val sparkSession: SparkSession = spark

val originalDf = df.select("id", "name", "age")
val schema = originalDf.schema

val result = for {
  toonChunks <- originalDf.toToon(key = "data")
  decodedDf <- SparkToonOps.fromToon(toonChunks, schema)
} yield decodedDf

result match {
  case Right(reconstructed) =>
    reconstructed.show()
  case Left(error) =>
    println(s"Error: ${error.message}")
}
```

### Chunking Large DataFrames

```scala
// Handle large datasets with automatic chunking
val largeDf = spark.read.parquet("large_dataset.parquet")

largeDf.toToon(
  key = "data",
  maxRowsPerChunk = 1000  // Process 1000 rows per chunk
) match {
  case Right(chunks) =>
    println(s"Created ${chunks.size} chunks")
    chunks.zipWithIndex.foreach { case (toon, idx) =>
      // Process each chunk independently
      saveToon(s"chunk_$idx.toon", toon)
    }
  case Left(error) =>
    println(s"Error: ${error.message}")
}
```

### LLM Integration (llm4s-compatible)

toon4s-spark provides an LLM client abstraction that mirrors [llm4s](https://github.com/llm4s/llm4s) design patterns for forward compatibility.

#### Conversation-Based API

```scala
import io.toonformat.toon4s.spark.llm._

// Create conversation
val conversation = for {
  sys <- Message.system("You are a data analyst")
  user <- Message.user("Analyze this data: ...")
  conv <- Conversation.create(sys, user)
} yield conv

// Use mock client for testing
val client = MockLlmClient.alwaysSucceeds

conversation.flatMap { conv =>
  client.complete(conv, CompletionOptions.default)
} match {
  case Right(completion) =>
    println(s"Response: ${completion.content}")
    println(s"Tokens: ${completion.usage}")
  case Left(error) =>
    println(s"Error: ${error.formatted}")
}
```

#### Send TOON Data to LLM

```scala
df.toToon(key = "analytics_data") match {
  case Right(toonChunks) =>
    toonChunks.foreach { toon =>
      val result = for {
        conv <- Conversation.fromPrompts(
          "You are a data analyst",
          s"Analyze this data:\n$toon"
        )
        completion <- client.complete(conv)
      } yield completion

      result.foreach { completion =>
        println(s"Analysis: ${completion.content}")
        completion.usage.foreach { usage =>
          println(s"Cost: ${usage.estimateCost(0.01, 0.03)}")
        }
      }
    }
  case Left(error) =>
    println(s"Encoding error: ${error.message}")
}
```

#### Backward-Compatible String API

```scala
// Simple string-based API for quick prototyping
val client = MockLlmClient(Map("test" -> "response"))

client.completeSimple("What is 2+2?") match {
  case Right(response) => println(response)
  case Left(error) => println(error.message)
}

// With system prompt
client.completeWithSystem(
  "You are helpful",
  "What is 2+2?"
) match {
  case Right(response) => println(response)
  case Left(error) => println(error.message)
}
```

#### Streaming Support

```scala
val result = for {
  conv <- Conversation.userOnly("Tell me a story")
  completion <- client.streamComplete(conv) { chunk =>
    chunk.content.foreach(print)  // Print each chunk as it arrives
  }
} yield completion
```

#### Context Window Management (llm4s pattern)

```scala
val client = MockLlmClient.alwaysSucceeds

// Check context limits
val contextWindow = client.getContextWindow()  // 128000 for GPT-4o
val reserved = client.getReserveCompletion()   // 4096 reserved for output

// Calculate available budget
val budget = client.getContextBudget(HeadroomPercent.Standard)
println(s"Available: ${budget.available} tokens")

// Check if prompt fits
if (budget.fits(promptTokens)) {
  // Send to LLM
}
```

## API Reference

### Extension Methods on DataFrame

**`toToon(key: String, maxRowsPerChunk: Int, options: EncodeOptions): Either[SparkToonError, Vector[String]]`**

Encode DataFrame to TOON format with chunking support.

- `key`: Top-level key for TOON document (default: "data")
- `maxRowsPerChunk`: Maximum rows per chunk (default: 1000)
- `options`: TOON encoding options (default: EncodeOptions())

**`toonMetrics(key: String, options: EncodeOptions): Either[SparkToonError, ToonMetrics]`**

Compute token metrics comparing JSON vs TOON efficiency.

**`showToonSample(n: Int): Unit`**

Print a sample of TOON-encoded data for debugging (default: 5 rows).

### Static Methods

**`SparkToonOps.fromToon(toonDocuments: Vector[String], schema: StructType, options: DecodeOptions)(implicit spark: SparkSession): Either[SparkToonError, DataFrame]`**

Decode TOON strings back to DataFrame.

### SQL UDFs

Register with `ToonUDFs.register(spark)`:

- `toon_encode_row(struct)`: Encode struct to TOON
- `toon_decode_row(string)`: Decode TOON to struct
- `toon_encode_string(string)`: Encode string value
- `toon_decode_string(string)`: Decode TOON string
- `toon_estimate_tokens(string)`: Estimate token count

### ToonMetrics

```scala
case class ToonMetrics(
  jsonTokenCount: Int,
  toonTokenCount: Int,
  savingsPercent: Double,
  rowCount: Int,
  columnCount: Int
)
```

Methods:
- `absoluteSavings: Int` - Token count difference
- `compressionRatio: Double` - TOON/JSON ratio
- `estimatedCostSavings(costPer1kTokens: Double): Double` - Cost savings estimate
- `hasMeaningfulSavings(threshold: Double): Boolean` - Check if savings exceed threshold
- `summary: String` - Formatted summary

### Error Handling

All operations return `Either[SparkToonError, A]` for explicit error handling:

```scala
sealed trait SparkToonError {
  def message: String
  def cause: Option[Throwable]
}

object SparkToonError {
  case class ConversionError(message: String, cause: Option[Throwable] = None)
  case class EncodingError(toonError: EncodeError, cause: Option[Throwable] = None)
  case class DecodingError(toonError: DecodeError, cause: Option[Throwable] = None)
  case class SchemaMismatch(expected: String, actual: String, cause: Option[Throwable] = None)
  case class CollectionError(message: String, cause: Option[Throwable] = None)
  case class UnsupportedDataType(dataType: String, cause: Option[Throwable] = None)
}
```

## Configuration

### Java 17+ Compatibility

For Java 17 or later, add these JVM options:

```scala
// build.sbt
Test / javaOptions ++= Seq(
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED"
)
```

### Spark Configuration

```scala
val spark = SparkSession.builder()
  .appName("TOON App")
  .config("spark.sql.shuffle.partitions", "100")
  .config("spark.executor.memory", "4g")
  .getOrCreate()
```

## Performance Tips

1. **Chunking**: Use `maxRowsPerChunk` to control memory usage for large DataFrames
2. **Caching**: Cache DataFrames before multiple TOON operations
3. **Partitioning**: Repartition for parallelism before encoding
4. **Token estimation**: Use `toonMetrics()` to verify savings before committing to TOON

```scala
val largeDf = spark.read.parquet("data.parquet")
  .repartition(100)  // Parallelize
  .cache()           // Cache for reuse

// Check if TOON provides meaningful savings
largeDf.toonMetrics() match {
  case Right(metrics) if metrics.hasMeaningfulSavings(threshold = 15.0) =>
    // Proceed with TOON encoding
    largeDf.toToon(maxRowsPerChunk = 1000)
  case Right(metrics) =>
    // Savings too small, use JSON
    println(s"TOON savings only ${metrics.savingsPercent}%, using JSON")
  case Left(error) =>
    println(s"Error: ${error.message}")
}
```

## Requirements

- Scala 2.12 or 2.13
- Apache Spark 3.5.0+
- Java 11+ (Java 17+ recommended)

## License

MIT License - See LICENSE file for details.
