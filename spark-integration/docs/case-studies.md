# Case studies

This page documents usage stories and reproducible outcomes for `toon4s-spark`.

## Study 1: Tabular analytics summary workflow

### Context

- Workload: Spark SQL aggregation over a tabular events table
- Data shape: flat columns, low nesting depth
- Target: generate compact LLM-ready payloads

### Baseline flow

- Query result encoded as JSON
- JSON passed to downstream LLM call

### TOON flow

- Same query result encoded with `df.toToon(ToonSparkOptions(...))`
- TOON chunks passed to downstream LLM call

### Reproducible method

1. Use the sample in `spark-integration/examples/SparkLlmEndToEndExample.scala`.
2. Run token comparison with `df.toonMetrics(ToonSparkOptions(...))`.
3. Compare:
   - JSON token count
   - TOON token count
   - savings percent
4. Repeat on your production-like dataset and record values.

### Notes

- For tabular and shallow data, benchmark findings indicate meaningful token savings.
- For deeply nested payloads, use JSON and treat TOON as opt-in only after alignment checks.

## Study 2: Streaming micro-batch encode path

### Context

- Workload: Spark Structured Streaming micro-batches
- Target: encode each batch and push to sink

### Reproducible method

1. Follow streaming sketch in `spark-integration/README.md`.
2. Use guardrails in strict mode.
3. Capture counters from `encodeWithGuardrails`:
   - total/success/failure counts
   - chunk size and estimated token distribution
4. Validate fallback behavior and warning logs.

### Notes

- Keep chunking settings stable between metrics and production encoding.
- Use strict mode first; move to lenient mode only with measured evidence.

## Study 3: Measured run on real CSV workload

### Context

- Source: `/Users/vim/Downloads/customers-2000000.csv` (about 333 MB)
- Run date: 2026-02-21
- Spark runtime: 4.1.1 local mode
- Workload mode: `agg` via `examples.WorkloadMeasurementRunner`

Full independent run note:

- `spark-integration/docs/WORKLOAD_MEASUREMENT_2026-02-22_independent.md`

### Result summary

- Rows after aggregation: 881
- JSON bytes: 89,203
- TOON bytes: 195,928
- Byte savings: **-119.64%** (TOON larger)
- JSON tokens: 22,378
- TOON tokens: 48,982
- Token savings: **-118.88%** (TOON larger)
- JSON encode time: 1,493 ms
- TOON encode time: 795 ms

### Why this matters

- This run proves TOON is not always better.
- For this specific shape, JSON is better for token cost.
- Keep TOON as an opt-in path after alignment checks and workload measurement.
- Keep benchmark claims tied to reproducible workload notes.

## Trademark notice

Apache Spark, Spark, and the Spark logo are trademarks of the Apache Software Foundation.
This project is an external third-party integration and is not affiliated with, endorsed by, or sponsored by the Apache Software Foundation.
