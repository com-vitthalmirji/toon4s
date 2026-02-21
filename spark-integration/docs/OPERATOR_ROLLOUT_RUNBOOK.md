# Operator rollout runbook

## Goal

Roll out toon4s-spark safely in production Spark jobs with a clear rollback path.

## Pre rollout checklist

- confirm Spark version is in `COMPATIBILITY_MATRIX.md`
- confirm CI is green for current release tag
- confirm migration notes reviewed
- set `ToonSparkOptions` explicitly in production code
- set chunk size for encode and metrics to the same value

## Rollout steps

1. deploy to staging with production-like data shape
2. run baseline metrics with JSON path
3. run TOON path on same workload window
4. compare token savings, error rate, and job duration
5. enable TOON for a small production slice
6. increase traffic gradually after stable runs

## Monitoring checkpoints

- token savings percent trend
- TOON encode and decode error counts
- stage or task failure count
- batch duration and throughput trends
- driver memory pressure during encoding

## Rollback triggers

- error rate above agreed threshold
- token savings below expected minimum for tabular workload
- job duration regression above agreed threshold
- repeated write path failures on storage layer

## Rollback steps

1. switch workflow back to JSON path
2. disable SQL extension if enabled
3. keep artifacts and logs for postmortem
4. open issue with commit and run ids
5. block further rollout until fix is validated

## Post rollback actions

- classify root cause
- add test or benchmark to prevent repeat
- update migration and compatibility docs if needed
