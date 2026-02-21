# Compatibility matrix

## Runtime support

| Area | Status |
|---|---|
| Spark 3.5.x | Supported |
| Spark 4.0.x | Supported |
| Scala 2.13 | Supported |
| Scala 3 for spark-integration module | Not supported in current Spark dependency model |
| JDK 21 | Supported in CI |

## CI coverage

| Dimension | Covered |
|---|---|
| OS | Linux, macOS, Windows |
| Spark compatibility checks | 3.5.0 and 4.0.1 |
| Main test matrix | Scala 2.13 and Scala 3 for root modules |

## Notes

- Published spark-integration artifact compiles against Spark 3.5.0 APIs.
- CI validates runtime compatibility on Spark 4.0.1.
- Windows DataSource V2 write tests depend on Hadoop `winutils` and are guarded in tests.
