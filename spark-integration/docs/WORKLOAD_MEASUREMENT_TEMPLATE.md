# Workload measurement note template

Use this template after running `examples.WorkloadMeasurementExample`.

## 1. Workload details

- Name:
- Run date:
- Environment:
  - Spark version:
  - Scala version:
  - Cluster mode:
- Source type:
  - Table:
  - or Parquet path:
- Query summary:

## 2. TOON settings

- `key`:
- `maxRowsPerChunk`:
- Guardrail mode:
- Fallback mode:

## 3. Measured output

- Rows:
- Chunk count:
- Avg chunk bytes:

- JSON bytes:
- TOON bytes:
- Byte savings percent:

- JSON estimated tokens:
- TOON estimated tokens:
- Token savings percent:

- JSON encode time ms:
- TOON encode time ms:
- TOON overhead vs JSON percent:

## 4. Cost estimate

- Cost model used:
- JSON estimated cost:
- TOON estimated cost:
- Estimated delta:

## 5. Findings

- What improved:
- What regressed:
- Unexpected behavior:
- Alignment warnings observed:

## 6. Decisions

- Keep defaults as-is?:
- Needed default changes:
- Needed guardrail changes:
- Follow-up tasks:

## 7. Repro steps

Paste exact command used:

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
  --csv /path/to/file.csv \
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

## Trademark notice

Apache Spark, Spark, and the Spark logo are trademarks of the Apache Software Foundation.
