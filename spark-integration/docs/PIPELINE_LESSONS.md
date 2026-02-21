# Pipeline Lessons and Hardening Notes

This document captures practical lessons from running `toon4s-spark` in a real Spark pipeline.

## Scope

- Target flow: batch query -> TOON chunks -> LLM or downstream sink
- Optional flow: streaming micro-batch -> TOON chunks -> sink
- Goal: confirm defaults, surface corner cases, and reduce production risk

## Required runtime signals

Capture these signals on every run:

- Input shape: row count, column count, nesting depth
- Guardrail decision: strict or lenient mode, allow or block
- Chunk stats: chunk count, min/max/avg bytes, estimated tokens per chunk
- Outcome counts: total encodes, successful encodes, failed encodes
- Fallback path: JSON fallback or skip, with warning message

## Dogfood checklist

1. Start with strict guardrails:
   - `minBytesPerChunk = 10 * 1024`
   - `maxRowsPerChunk = 1000`
   - `mode = Strict`
2. Run one full batch path and capture guardrail counters.
3. Run one streaming micro-batch path and validate chunk counts.
4. Review warnings for alignment, deep nesting, and fallback usage.
5. Tune only one default at a time, then rerun and compare counters.

## Lessons template

Fill this block after each pipeline run:

- Date:
- Pipeline:
- Spark/Scala versions:
- Data shape:
- Guardrail settings:
- Outcome:
  - Total encodes:
  - Successful encodes:
  - Failed encodes:
  - Fallback mode usage:
- Problems found:
- Fix applied:
- Default changes (if any):

## Current status

- Guardrail API and counters are in place.
- JSON fallback and skip fallback both emit warnings.
- Remaining work: add one production-backed case study with measured results.

## Large-file incident log 2026-02-21

### What failed

1. Wrong runner class in command examples
   - Symptom: `ClassNotFoundException: examples.WorkloadMeasurementExample`
   - Root cause: command used old class name while runnable entry was `examples.WorkloadMeasurementRunner`
   - Fix: align commands and docs to the actual runner class

2. Runtime method mismatch in Spark 4.1.1 run
   - Symptom: `NoSuchMethodError` around Spark implicits in measurement summary path
   - Root cause: typed Dataset conversion path was less stable across Spark runtime combinations
   - Fix: use DataFrame aggregate + `collect().head` in summary path, avoid that implicits call

3. Shell command parse issue
   - Symptom: `zsh: no matches found: local[*]`
   - Root cause: unquoted bracket pattern in shell
   - Fix: run with quoted master value: `--master 'local[*]'`

4. Real workload result contradicted default expectation
   - Symptom: TOON was larger than JSON on one 2M-row CSV aggregate workload
   - Root cause: workload shape and resulting payload favored JSON tokenization
   - Fix: document measured evidence and make TOON guidance explicitly workload-driven

### Why this happened before

- Early integration optimized for API ergonomics and tabular benchmark sweet spots.
- Most examples used convenience return types first, so large-file negative paths were under-documented.
- Harness for real-workload proof existed but was not enforced in docs and release checklist.

### Why RDD APIs were not the primary public surface before

- Public Spark API was intentionally exposed as DataFrame/Dataset extensions for Spark user ergonomics.
- Internally, distributed work already used partition-level operations (`mapPartitions`, `rdd.mapPartitionsWithIndex`).
- The gap was not “no RDD usage”; the gap was that docs and usage patterns still favored convenience methods in some places.
- We now treat `toToonDataset`, `writeToon`, and `writeToLlmPartitions` as production paths for large data.

### What we changed for large-file safety

- Production guidance now favors distributed paths over collection-style paths.
- Measurement docs now include a real large-file run with exact command and output.
- Readiness docs now include explicit negative-result handling (JSON can win).

## Trademark notice

Apache Spark, Spark, and the Spark logo are trademarks of the Apache Software Foundation.
This project is an external third-party integration and is not affiliated with, endorsed by, or sponsored by the Apache Software Foundation.
