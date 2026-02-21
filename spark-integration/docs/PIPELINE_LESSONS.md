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

## Trademark notice

Apache Spark, Spark, and the Spark logo are trademarks of the Apache Software Foundation.
This project is an external third-party integration and is not affiliated with, endorsed by, or sponsored by the Apache Software Foundation.
