# Workload measurement 2026-02-22 independent run

## Goal

Independent rerun of JSON vs TOON payload metrics for `toon4s-spark` issue #64.

## Environment

- Repository commit: `297f9da0e77c34a498f95d1fc50dca049eabbdb5`
- toon4s version: `0.8.1`
- Spark version: `4.1.1`
- Scala version: `2.13.17`
- Java version: `21.0.10`
- sbt version: `1.11.7`
- Runtime mode: `spark-submit --master local[*]`

## Dataset shape

- Source file: `/Users/vim/Downloads/customers-2000000.csv`
- File size: about `333 MB`
- Source rows: `2,000,000` data rows (`2,000,001` lines including header)
- Source columns: `12`
- Workload mode: `agg` (grouped metric summary)
- Aggregated output rows: `881`

## Exact run command

```bash
sbt "core/package" "sparkIntegration/package"
spark-submit \
  --class examples.WorkloadMeasurementRunner \
  --master 'local[*]' \
  --jars core/target/scala-2.13/toon4s-core_2.13-0.7.0-19-3e0fe95c-20260221-1554-SNAPSHOT.jar \
  spark-integration/target/scala-2.13/toon4s-spark_2.13-0.8.1.jar \
  --csv /Users/vim/Downloads/customers-2000000.csv \
  --mode agg \
  --key customers \
  --maxRowsPerChunk 1000
```

## Measured output

- rows: `881`
- jsonBytes: `89,203`
- toonBytes: `195,928`
- bytesSavingsPct: `-119.64%`
- jsonTokens: `22,378`
- toonTokens: `48,982`
- tokenSavingsPct: `-118.88%`
- jsonEncodeMs: `1,418`
- toonEncodeMs: `740`
- overheadPctVsJson: `-47.81%`
- chunkCount: `1`
- avgChunkBytes: `195,928`

## Throughput summary

Computed from `rows / encodeMs` on the aggregated output:

- JSON throughput: `621.30 rows/sec`
- TOON throughput: `1190.54 rows/sec`

## Result

For this workload shape, JSON is better on token and byte cost.
TOON was faster to encode in this run, but larger in payload size and token estimate.
