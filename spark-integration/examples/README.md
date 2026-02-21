# TOON Spark Integration Examples

This directory contains production-ready examples demonstrating toon4s-spark integration with modern data lakehouse platforms.

## Examples Overview

| Example | Use Case | Key Features | Platform |
|---------|----------|--------------|----------|
| **TryIn5MinutesExample** | First successful run | Local batch query, TOON encode, first chunk output | Local Spark |
| **SparkLlmEndToEndExample** | Query to LLM flow | SQL aggregation, TOON encoding, LLM stub call | Local Spark |
| **WorkloadMeasurementExample** | Real workload proof | JSON vs TOON bytes, tokens, timing, chunk stats | Local Spark / Cluster |
| **DatabricksStreamingExample** | Real-time fraud detection | Delta Lake CDC, streaming TOON encoding, health monitoring | Databricks + Delta Lake |
| **IcebergTrendAnalysisExample** | Quarterly business trends | Time travel, multi-snapshot comparison, LLM analysis | Apache Iceberg |
| **ProductionMonitoringExample** | Pre-deployment validation | Health checks, telemetry, schema drift detection | Any Spark cluster |

## Prerequisites

### Software Requirements

- **Apache Spark 3.5.0+** (Scala 2.13)
- **Java 11+** (Java 17+ recommended)
- **Delta Lake 3.0+** (for Databricks example)
- **Apache Iceberg 1.4+** (for Iceberg example)
- **toon4s-spark 0.1.0+**

### Benchmark Context

All examples are based on [TOON Generation Benchmark](https://github.com/vetertann/TOON-generation-benchmark) findings:

- ✅ **TOON wins**: Tabular data (90.5% accuracy, 22% token savings)
- ❌ **TOON fails**: Deep hierarchies (0% one-shot accuracy)
- ⚠️ **Prompt tax**: TOON overhead > savings for small datasets (< 1KB)

## Running Examples

### 1. Try in 5 minutes

**Run**:

```bash
spark-submit \
  --class examples.TryIn5MinutesExample \
  --master local[*] \
  toon4s-spark-assembly.jar
```

### 2. Spark + LLM end-to-end stub

**Run**:

```bash
spark-submit \
  --class examples.SparkLlmEndToEndExample \
  --master local[*] \
  toon4s-spark-assembly.jar
```

### 3. Workload measurement harness

**Run with a table**:

```bash
spark-submit \
  --class examples.WorkloadMeasurementExample \
  --master local[*] \
  toon4s-spark-assembly.jar \
  --table analytics.events \
  --key workload \
  --maxRowsPerChunk 1000
```

**Run with a parquet file**:

```bash
spark-submit \
  --class examples.WorkloadMeasurementExample \
  --master local[*] \
  toon4s-spark-assembly.jar \
  --parquet /data/events.parquet \
  --key workload \
  --maxRowsPerChunk 1000
```

Use `spark-integration/docs/WORKLOAD_MEASUREMENT_TEMPLATE.md` to store your run note.

**Run with a csv file**:

```bash
spark-submit \
  --class examples.WorkloadMeasurementExample \
  --master local[*] \
  toon4s-spark-assembly.jar \
  --csv /data/customers.csv \
  --key workload \
  --maxRowsPerChunk 1000
```

### 4. Databricks Streaming Example

**Scenario**: Financial institution monitors transaction table for fraud patterns using real-time LLM analysis.

**Setup**:

```sql
-- Enable Change Data Feed on Delta table
CREATE TABLE transactions (
  txn_id STRING,
  user_id STRING,
  amount DOUBLE,
  merchant STRING,
  timestamp TIMESTAMP
) USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Insert test data
INSERT INTO transactions VALUES
  ('txn_001', 'user_123', 1500.00, 'Electronics Store', current_timestamp()),
  ('txn_002', 'user_456', 50.00, 'Coffee Shop', current_timestamp());
```

**Run**:

```bash
spark-submit \
  --class examples.DatabricksStreamingExample \
  --master yarn \
  --deploy-mode cluster \
  --packages io.delta:delta-core_2.13:3.0.0 \
  toon4s-spark-assembly.jar
```

**What It Demonstrates**:

1. **Schema validation**: Pre-flight check before streaming
2. **CDC streaming**: Real-time TOON encoding of Delta change events
3. **Adaptive chunking**: Optimize prompt tax for micro-batches
4. **LLM integration**: Send TOON-encoded fraud patterns to LLM
5. **Production monitoring**: Collect telemetry and track alignment scores

**Key Code**:

```scala
val config = DeltaCDCConfig(
  tableName = "transactions",
  checkpointLocation = "/dbfs/checkpoints/fraud-detection",
  triggerInterval = "30 seconds"
)

val query = streamDeltaCDCWithMetadata(config) { metadata =>
  // Validate alignment score
  println(s"Alignment: ${metadata.alignmentScore}")

  // Process TOON chunks
  metadata.toonChunks.foreach { toon =>
    llmClient.analyze(toon) // Send to LLM
  }
}

query.awaitTermination()
```

### 5. Iceberg Time Travel Example

**Scenario**: E-commerce company analyzes quarterly customer trends using historical Iceberg snapshots.

**Setup**:

```scala
// Iceberg table configuration
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.spark_catalog.type", "hive")
spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
```

```sql
CREATE TABLE customer_metrics (
  customer_id STRING,
  total_orders INT,
  total_spend DOUBLE,
  avg_order_value DOUBLE,
  last_purchase_date DATE
) USING ICEBERG;
```

**Run**:

```bash
spark-submit \
  --class examples.IcebergTrendAnalysisExample \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.4.0 \
  toon4s-spark-assembly.jar
```

**What It Demonstrates**:

1. **Time travel queries**: Read historical snapshots at specific timestamps
2. **Multi-snapshot comparison**: Compare Q1, Q2, Q3, Q4 in single LLM prompt
3. **Schema consistency**: Validate alignment across time ranges
4. **Time series generation**: Weekly snapshots for momentum analysis
5. **Production readiness**: Generate comprehensive reports before deployment

**Key Code**:

```scala
// Compare Q3 vs Q4
val comparison = compareSnapshots(
  tableName = "customer_metrics",
  beforeTimestamp = Instant.parse("2024-09-30T23:59:59Z"),
  afterTimestamp = Instant.parse("2024-12-31T23:59:59Z")
)

comparison.foreach { case (q3Data, q4Data) =>
  val prompt = s"""
    Analyze changes from Q3 to Q4:
    Q3: ${q3Data.mkString("\n")}
    Q4: ${q4Data.mkString("\n")}
  """
  llmClient.analyze(prompt)
}

// Generate weekly time series
val timeSeries = generateSnapshotTimeSeries(
  tableName = "customer_metrics",
  startTime = q4Start,
  endTime = q4End,
  intervalSeconds = 7 * 24 * 3600 // Weekly
)
```

### 6. Production Monitoring Example

**Scenario**: Pre-deployment validation and runtime monitoring for production TOON encoding.

**Run**:

```bash
spark-submit \
  --class examples.ProductionMonitoringExample \
  --master local[*] \
  toon4s-spark-assembly.jar
```

**What It Demonstrates**:

1. **Health assessment**: Pre-flight checks before production deployment
2. **Production readiness reports**: Comprehensive stakeholder reports
3. **Performance measurement**: Baseline encoding metrics and token savings
4. **Telemetry collection**: Lightweight metrics for dashboards (Datadog, Prometheus)
5. **Schema drift detection**: Monitor alignment score changes over time
6. **Alerting thresholds**: Configure production alerts

**Key Code**:

```scala
// Phase 1: Pre-deployment validation
val health = assessDataFrameHealth(df, "production.events")

if (health.productionReady) {
  println(s"✅ ${health.summary}")
  df.toToon(maxRowsPerChunk = health.chunkStrategy.chunkSize)
} else {
  println(s"❌ Blocking issues: ${health.issues}")
  // Fall back to JSON
}

// Phase 2: Generate production report
val report = generateProductionReport(df, "production.events")
Files.write(Paths.get("readiness-report.md"), report.getBytes)

// Phase 3: Collect runtime telemetry
val telemetry = collectTelemetry(df, "production.events")
statsd.gauge("toon.alignment_score", telemetry.alignmentScore)

// Phase 4: Measure encoding performance
val metrics = measureEncodingPerformance(df)
println(s"Encoding: ${metrics.encodingTimeMs}ms")
println(s"Savings: ${metrics.savingsPercent}%")

// Phase 5: Detect schema drift
val scoreDrift = evolvedScore - originalScore
if (scoreDrift > 0.1) {
  alertOncall("Schema drift detected!")
}
```

## Production Deployment Checklist

Before deploying TOON encoding to production:

### 1. Schema Validation

```scala
val alignment = ToonAlignmentAnalyzer.analyzeSchema(df.schema)
assert(alignment.aligned, s"Schema not TOON-aligned: ${alignment.recommendation}")
```

**Red flags**:
- ❌ Nesting depth > 3 levels (0% one-shot accuracy in benchmark)
- ❌ Alignment score < 0.7 (poor generation quality)
- ❌ Deep hierarchies with arrays (array count mismatches)

### 2. Prompt Tax Optimization

```scala
val strategy = AdaptiveChunking.calculateOptimalChunkSize(df)

if (!strategy.useToon) {
  println(s"⚠️ ${strategy.reasoning}")
  // Use JSON instead
}
```

**Red flags**:
- ❌ Dataset < 1KB (prompt overhead > syntax savings)
- ⚠️ Dataset < 10KB (marginal savings, monitor closely)

### 3. Token Savings Validation

```scala
val metrics = df.toonMetrics()
assert(metrics.savingsPercent > 15.0, "Insufficient token savings")
```

**Expected savings** (from benchmark):
- ✅ Tabular data: 20-25% savings
- ✅ Shallow nesting: 15-20% savings
- ⚠️ Medium nesting: 10-15% savings
- ❌ Deep nesting: 0-5% savings (use JSON)

### 4. Production Monitoring

```scala
// Collect telemetry every 5 minutes
val telemetry = collectTelemetry(df, tableName)

// Send to monitoring system
statsd.gauge("toon.alignment_score", telemetry.alignmentScore,
  tags = Seq(s"table:$tableName"))
statsd.gauge("toon.max_depth", telemetry.maxDepth,
  tags = Seq(s"table:$tableName"))
```

**Alert thresholds**:
- 🚨 Alignment score < 0.7
- 🚨 Nesting depth > 3
- 🚨 Token savings < 15%
- 🚨 Encoding failure rate > 5%

### 5. Production Readiness Report

```scala
val report = generateProductionReport(df, tableName)
Files.write(Paths.get("readiness-report.md"), report.getBytes)

// Review with stakeholders before deployment
```

## Integration with Monitoring Systems

### Datadog

```scala
val telemetry = collectTelemetry(df, tableName)

statsd.gauge("toon.alignment_score", telemetry.alignmentScore,
  tags = Seq(s"table:$tableName", s"env:production"))
statsd.gauge("toon.row_count", telemetry.rowCount,
  tags = Seq(s"table:$tableName"))
statsd.histogram("toon.encoding_time_ms", metrics.encodingTimeMs,
  tags = Seq(s"table:$tableName"))
```

### Prometheus

```scala
val telemetry = collectTelemetry(df, tableName)

prometheus.gauge("toon_alignment_score")
  .labels(tableName, "production")
  .set(telemetry.alignmentScore)

prometheus.gauge("toon_row_count")
  .labels(tableName)
  .set(telemetry.rowCount)
```

### CloudWatch

```scala
cloudwatch.putMetric(
  namespace = "TOON",
  metricName = "AlignmentScore",
  value = telemetry.alignmentScore,
  dimensions = Map(
    "TableName" -> tableName,
    "Environment" -> "production"
  )
)
```

## Troubleshooting

### Issue: Low token savings (< 15%)

**Diagnosis**:
```scala
val alignment = analyzeSchema(df.schema)
println(s"Max depth: ${alignment.maxDepth}") // Check nesting
println(s"Warnings: ${alignment.warnings}") // Review issues
```

**Solutions**:
1. Flatten schema (reduce nesting depth)
2. Use JSON encoding instead of TOON
3. Filter columns to reduce complexity

### Issue: Encoding failures in production

**Diagnosis**:
```scala
val health = assessDataFrameHealth(df, tableName)
println(health.issues) // Blocking issues
println(health.warnings) // Non-blocking warnings
```

**Solutions**:
1. Validate schema alignment before deployment
2. Monitor alignment score drift (schema evolution)
3. Set up alerts for alignment < 0.7

### Issue: Schema drift over time

**Diagnosis**:
```scala
// Track schema changes
val baselineHash = computeSchemaHash(originalSchema)
val currentHash = computeSchemaHash(currentSchema)

if (baselineHash != currentHash) {
  val newAlignment = analyzeSchema(currentSchema)
  println(s"Alignment drift: ${newAlignment.score - baselineScore}")
}
```

**Solutions**:
1. Re-validate alignment after schema changes
2. Set up schema change alerts
3. Test TOON encoding with new schema before production

## Performance Tips

### 1. Use Adaptive Chunking

```scala
val strategy = calculateOptimalChunkSize(df)
df.toToon(maxRowsPerChunk = strategy.chunkSize)
```

### 2. Cache DataFrames

```scala
val cachedDf = df.cache()
cachedDf.toToon() // Fast encoding
```

### 3. Repartition for Parallelism

```scala
val repartitionedDf = df.repartition(100)
repartitionedDf.toToon() // Parallel encoding
```

### 4. Pre-flight Validation

```scala
// Quick check before expensive operations
if (shouldUseToon(df)) {
  df.toToon()
} else {
  df.toJSON.take(1000) // Fall back to JSON with bounded sample
}
```

## References

- [TOON Generation Benchmark](https://github.com/vetertann/TOON-generation-benchmark) - Benchmark results and methodology
- [TOON Format Specification](https://toon.format) - Official TOON format docs
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/) - Spark 3.5 reference
- [Delta Lake Documentation](https://docs.delta.io/) - Delta Lake CDC and streaming
- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/) - Iceberg time travel

## Support

For issues or questions:
- GitHub Issues: https://github.com/vitthalmirji/toon4s/issues
- TOON Benchmark Discussion: https://github.com/vetertann/TOON-generation-benchmark/discussions
