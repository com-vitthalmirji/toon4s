# Claims traceability

This file maps public claims to concrete evidence.

## Current claims map

| Claim | Location | Evidence |
|---|---|---|
| tabular case can save around 22 percent tokens | `spark-integration/README.md` overview and benchmark table | external benchmark repository `TOON-generation-benchmark` |
| Spark compatibility includes 3.5.0 and 4.0.1 | `spark-integration/README.md` and `COMPATIBILITY_MATRIX.md` | CI jobs Spark compatibility 3.5.0 and 4.0.1 |
| stable patch API includes ToonSparkOptions entry points | `API_STABILITY_POLICY.md` | source methods in SparkToonOps and SparkDatasetOps |
| metrics alignment should use same chunk size as encode | `MIGRATION_GUIDE.md` | implementation and tests in SparkToonOps and SparkDatasetOps |

## Evidence update rules

- Every new performance claim needs either:
  - a new benchmark report from `BENCHMARK_REPORT_TEMPLATE.md`, or
  - a clear external source link with scope and date.
- Every compatibility claim needs a CI job reference.
- Every API stability claim needs source reference in this repository.

## Next report placeholder

- planned report file:
- planned commit:
- planned date:
