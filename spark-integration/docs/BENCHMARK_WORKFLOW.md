# Benchmark workflow

This workflow makes token and throughput claims reproducible.

## Goal

- Reproduce core encode decode throughput.
- Reproduce Spark side token savings and encode throughput.
- Record environment and command lines with the result.

## Environment checklist

- OS name and version
- CPU model and core count
- RAM size
- JDK version
- sbt version
- Spark version

## Core throughput benchmark with JMH

Run from repository root:

```bash
sbt "jmh/jmh:run -i 5 -wi 5 -f1 -t1 io.toonformat.toon4s.jmh.EncodeDecodeBench"
```

Optional single benchmark:

```bash
sbt "jmh/jmh:run -i 5 -wi 5 -f1 -t1 io.toonformat.toon4s.jmh.EncodeDecodeBench.encode_real_world"
```

Capture output in a file:

```bash
sbt "jmh/jmh:run -i 5 -wi 5 -f1 -t1 io.toonformat.toon4s.jmh.EncodeDecodeBench" > benchmark-jmh.txt
```

## Spark token and throughput benchmark

Use spark integration monitoring helper on a representative DataFrame.

Example:

```scala
import io.toonformat.toon4s.spark.monitoring.ToonMonitoring._

val metricsResult = measureEncodingPerformance(df, key = "data", chunkSize = 1000)
```

Capture:

- row count
- column count
- total duration ms
- throughput rows per second
- json token count
- toon token count
- savings percent

## Dataset profiles to run

- Wide tabular data, depth 0 or 1
- Shallow nested data, depth 2
- Medium nested data, depth 3
- Deep nested data, depth 4 plus

## Reporting rules

- Include exact command lines.
- Include dataset shape and row count.
- Include raw outputs for JMH and Spark metrics.
- Keep old reports for trend tracking.

## Release gating rule

- Do not change README benchmark claims without a new benchmark report.
- Link new report in `CLAIMS_TRACEABILITY.md`.
