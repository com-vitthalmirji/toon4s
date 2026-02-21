# API stability policy

This policy defines what stays stable in patch releases of toon4s for Apache Spark.

## Stable in patch releases

- `ToonSparkOptions`
- `DataFrame.toToon(options: ToonSparkOptions)`
- `DataFrame.toonMetrics(options: ToonSparkOptions)`
- `Dataset[T].toToon(options: ToonSparkOptions)`
- `Dataset[T].toonMetrics(options: ToonSparkOptions)`
- Data source format name `toon`
- SQL extension provider class `io.toonformat.toon4s.spark.extensions.ToonSparkSessionExtensions`
- Monitoring guardrails API under `io.toonformat.toon4s.spark.monitoring.ToonMonitoring`

## Compatibility commitment

- Patch releases keep source compatibility for the stable APIs above.
- Behavior fixes are allowed when they remove incorrect behavior or unsafe behavior.
- New optional APIs can be added in patch releases.

## Deprecation rules

- Deprecations are announced in README and release notes.
- Deprecated API stays for at least one minor release before removal.
- Removal only happens in a new minor release.

## Non goals in patch releases

- No rename of stable API methods
- No parameter removal in stable methods
- No data source short name change

## Known platform limit

- Windows write path tests for DataSource V2 require Hadoop `winutils`.
- CI skips those two write path tests on Windows until a managed `winutils` path is available.

Trademark notice: Apache Spark and Spark are trademarks of The Apache Software Foundation.
