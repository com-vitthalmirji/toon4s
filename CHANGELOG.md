# Changelog

## [0.1.0] - 2025-11-06

### Added
- Spec-complete Scala 3 encoder/decoder (`toon4s-core`) with zero external dependencies.
- CLI (`toon4s-cli`) with JSON ⇄ TOON conversion, delimiter options, strict-mode controls, and a `--stats` flag for GPT token estimates.
- Pure Scala JSON codec (`SimpleJson`) used by both the library and CLI.
- Conformance test harness importing the official fixtures from `toon-format/spec`.
- Smoke-test script (`smoke-tests/run-smoke.sh`) to exercise the staged CLI on a real data set.
- sbt-native-packager configuration for shipping CLI distributions.
- CI workflow executing `sbt test` on pushes and pull requests.
- Comprehensive README with usage examples and release guidance.

### Reliability & Quality
- Deterministic object ordering via `VectorMap` to match reference outputs.
- Strict validation for array lengths, blank lines, and indentation identical to the TypeScript and Java implementations.
- Normalisation handles Scala maps/sets, non-string keys, big integers, and `java.time` / `java.util.Date` values.
- Full round-trip unit tests for tabular, nested, and delimiter-sensitive structures.
