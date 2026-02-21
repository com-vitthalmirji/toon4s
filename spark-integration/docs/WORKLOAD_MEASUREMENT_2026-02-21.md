# Workload measurement note 2026-02-21

## Workload details

- Name: customers csv aggregation
- Run date: 2026-02-21
- Environment:
  - Spark version: 4.1.1
  - Scala module: 2.13
  - Cluster mode: local
- Source type:
  - CSV path: `/Users/vim/Downloads/customers-2000000.csv`
- Query summary:
  - Use `examples.WorkloadMeasurementRunner` with `--mode agg`
  - Build grouped tabular output before JSON/TOON comparison

## TOON settings

- `key`: `customers`
- `maxRowsPerChunk`: `1000`
- Mode: `agg`

## Repro command

```bash
spark-submit --master 'local[*]' \
  --class examples.WorkloadMeasurementRunner \
  --packages com.vitthalmirji:toon4s-core_2.13:0.7.0 \
  spark-integration/target/scala-2.13/toon4s-spark_2.13-0.7.0-19-3e0fe95c-20260221-1711-SNAPSHOT.jar \
  --csv /Users/vim/Downloads/customers-2000000.csv \
  --mode agg \
  --key customers \
  --maxRowsPerChunk 1000
```

## Measured output

- Rows: `881`
- Chunk count: `1`
- Avg chunk bytes: `195928.00`

- JSON bytes: `89203`
- TOON bytes: `195928`
- Byte savings percent: `-119.64%`

- JSON estimated tokens: `22378`
- TOON estimated tokens: `48982`
- Token savings percent: `-118.88%`

- JSON encode time ms: `1493`
- TOON encode time ms: `795`
- TOON overhead vs JSON percent: `-46.75%`

## Findings

- On this workload, JSON wins for token and bytes.
- TOON encode path was faster on wall-clock in this local run.
- Keep TOON enablement behind alignment checks and workload-level measurement.

## Decision

- Do not claim universal token savings for `toon4s-spark`.
- Keep “TOON vs JSON” guidance explicit in README and docs.
- Require a workload note before changing public benchmark claims.

## Trademark notice

Apache Spark, Spark, and the Spark logo are trademarks of the Apache Software Foundation.
This project is an external third-party integration and is not affiliated with, endorsed by, or sponsored by the Apache Software Foundation.
