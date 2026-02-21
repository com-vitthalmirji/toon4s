# Review outcome closure

This file maps each gap from `docs/internals/REVIEW_OUTCOME_TOON4S_SPARK.md` to current status.

## Gap status

1. Core MiMa not active  
Status: done  
Evidence: `build.sbt` has active `mimaPreviousArtifacts` for core and spark modules.

2. No property tests in spark module  
Status: done  
Evidence: `SparkJsonInteropPropertyTest` covers primitive, nested, and temporal/binary paths.

3. Dual LLM abstraction confusion  
Status: done  
Evidence: production path is `writeToLlmPartitions`; legacy path is explicit via `fromLegacyClientFactory`.

4. ToonMonitoring god object  
Status: done  
Evidence: monitoring split into focused files under `spark/monitoring/`.

5. No prompt engineering guidance  
Status: done  
Evidence: `spark-integration/docs/PROMPT_ENGINEERING.md`.

6. No LLM response validation harness  
Status: done  
Evidence: `ToonLlmResponseValidator` and tests.

7. Token estimation hardcoded  
Status: done  
Evidence: `ToonMetrics.TokenEstimator` and configurable estimator wiring.

8. No spark-shell quick start  
Status: done  
Evidence: quick check section in `spark-integration/README.md`.

9. Single maintainer risk  
Status: mitigated in repo  
Evidence: `MAINTAINERS.md`, issue templates, contributor flow.  
Note: full risk closure requires additional active maintainers.

10. Delta/Iceberg integration maturity  
Status: done for code-level hardening  
Evidence: expanded tests in `DeltaLakeCDCTest` and `IcebergTimeTravelTest`.

11. No parser fuzz testing  
Status: done  
Evidence: `core/src/test/scala/io/toonformat/toon4s/ParserFuzzSpec.scala`.

## Strategic items from review plan

- Independent benchmark reproduction: protocol added in `BENCHMARK_REPRODUCIBILITY.md`.
- Spark website submission pack: `SPARK_WEBSITE_SUBMISSION.md`.
- Upstream proposal prep: `UPSTREAM_PROPOSAL_PREP.md`.
- Outreach package: `OUTREACH_ASSETS.md`.

## Final note

All in-repo actions from the review outcome are implemented.  
Remaining work is execution in external systems (Spark website PR, external benchmark runs, community adoption).
