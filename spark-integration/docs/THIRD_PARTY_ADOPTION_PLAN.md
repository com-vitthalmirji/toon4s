# toon4s-spark third party adoption plan

This plan tracks the path from useful integration to credible third party Spark integration.

## Target state

toon4s-spark is trusted by Spark users for LLM table workflows and is safe to list as a third party project in Spark docs.

## P0 release baseline

Scope:
- Stable API surface declaration for patch releases
- Compatibility statement for Spark, Scala, JDK
- Known limits section with clear operator guidance
- CI checks green across Linux, macOS, Windows

Exit criteria:
- API stability policy published
- Compatibility matrix published
- Migration guide published
- Current patch line green in CI

## P1 compatibility and migration confidence

Scope:
- Version matrix with support policy
- Upgrade notes between patch releases
- Deprecation and removal policy

Exit criteria:
- Migration guide includes examples for all stable entry points
- Breaking behavior list is explicit
- Release notes template includes migration section

## P2 quality and performance credibility

Scope:
- Reproducible benchmark workflow and report template
- Token savings and throughput numbers documented with setup
- Integration examples for local Spark and managed Spark

Exit criteria:
- Benchmark docs include command lines and environment
- Results include at least one wide table and one nested table case
- Performance claims in README link to benchmark report

## P3 ecosystem adoption readiness

Scope:
- Third party listing payload ready for Spark website
- Operator docs for rollout, rollback, and monitoring
- Release playbook for regular patch cadence

Exit criteria:
- Third party description text finalized
- Maintainer and support contact data finalized
- Adoption checklist completed for one real user workload
