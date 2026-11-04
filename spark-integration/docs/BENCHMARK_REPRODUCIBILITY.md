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

### Turning a run log into JSON and a table row

Save the run's console output, then parse it:

```bash
spark-submit --class examples.WorkloadMeasurementRunner <jar> --csv /path/to/file.csv --key workload \
  > workload-run.log 2>&1
scripts/parse-workload-measurement.sh --header workload-run.log
```

The script reads the `=== TOON workload measurement ===` block and ignores the Spark and sbt log lines
around it. It prints one JSON object (for a results file or a CI artifact) and a markdown row in the
order of the template's "Measured output" section. `--header` adds the header and separator rows:

```
{"rows": 2000, "chunkCount": 4, "avgChunkBytes": 12132.75, "jsonBytes": 108407, "toonBytes": 48531, "bytesSavingsPct": 55.23, "jsonTokens": 27325, "toonTokens": 12134, "tokenSavingsPct": 55.59, "jsonEncodeMs": 342, "toonEncodeMs": 100, "overheadPctVsJson": -70.76}
| Rows | Chunks | Avg chunk bytes | JSON bytes | TOON bytes | Byte savings % | JSON tokens | TOON tokens | Token savings % | JSON encode ms | TOON encode ms | TOON overhead % |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2000 | 4 | 12132.75 | 108407 | 48531 | 55.23 | 27325 | 12134 | 55.59 | 342 | 100 | -70.76 |
```

Percentages are printed without the `%` sign. When a log holds several runs, the last block is used. The
script exits non-zero, naming the field, when the block is missing or incomplete, so it can gate a CI step.

Reference example note:

- `spark-integration/docs/WORKLOAD_MEASUREMENT_2026-02-21.md`

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
