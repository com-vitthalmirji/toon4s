# Benchmark reproducibility

This page only describes how to reproduce benchmark numbers locally.

## Environment to record

- OS and version
- CPU model
- RAM
- JDK version
- sbt version
- Spark version

## Core JMH benchmark

Run from repository root:

```bash
sbt "jmh/jmh:run -i 5 -wi 5 -f1 -t1 io.toonformat.toon4s.jmh.EncodeDecodeBench"
```

Run one benchmark method:

```bash
sbt "jmh/jmh:run -i 5 -wi 5 -f1 -t1 io.toonformat.toon4s.jmh.EncodeDecodeBench.encode_real_world"
```

## Spark integration benchmark style run

Use a representative DataFrame and call:

```scala
import io.toonformat.toon4s.spark.monitoring.ToonMonitoring._

measureEncodingPerformance(df, key = "data", maxRowsPerChunk = Some(1000))
```

Record:

- row count and column count
- elapsed time
- rows per second
- JSON token count
- TOON token count
- savings percent

## Workload-level measurement run

Use the dedicated harness for one real workload:

```bash
spark-submit \
  --class examples.WorkloadMeasurementExample \
  <jar> \
  --table <table_name> \
  --key workload \
  --maxRowsPerChunk 1000
```

or

```bash
spark-submit \
  --class examples.WorkloadMeasurementExample \
  <jar> \
  --parquet /path/to/file.parquet \
  --key workload \
  --maxRowsPerChunk 1000
```

or

```bash
spark-submit \
  --class examples.WorkloadMeasurementExample \
  <jar> \
  --csv /path/to/file.csv \
  --key workload \
  --maxRowsPerChunk 1000
```

Store findings in `spark-integration/docs/WORKLOAD_MEASUREMENT_TEMPLATE.md`.

Reference example note:

- `spark-integration/docs/WORKLOAD_MEASUREMENT_2026-02-21.md`
- `spark-integration/docs/WORKLOAD_MEASUREMENT_2026-02-22_independent.md`

## Rules for claim updates

- Do not update public benchmark claims without rerunning benchmarks.
- Keep command lines and environment details with results.

## Independent reproduction protocol

Use this when an external engineer reruns the benchmark.

### Required inputs

- Repository commit hash.
- Spark version and Scala version.
- Dataset source and schema summary.
- Exact command used.

### Required outputs

- Raw run log.
- JSON vs TOON bytes.
- JSON vs TOON estimated tokens.
- Elapsed time and rows per second.
- Short conclusion: TOON wins / JSON wins / tie for this workload.

### Publish checklist

1. Put the result file under `spark-integration/docs/` as `WORKLOAD_MEASUREMENT_<date>_<owner>.md`.
2. Link to that file from `spark-integration/docs/case-studies.md`.
3. If the result changes recommendation guidance, update README in the same PR.

Trademark notice: Apache Spark and Spark are trademarks of The Apache Software Foundation.
