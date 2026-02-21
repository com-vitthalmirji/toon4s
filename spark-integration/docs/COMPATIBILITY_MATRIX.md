# Compatibility matrix

## Tested version pairs

| Spark | Scala module                    | JDK | CI status |
|-------|---------------------------------|-----|-----------|
| 3.5.0 | spark-integration on Scala 2.13 | 21  | Tested    |
| 4.0.1 | spark-integration on Scala 2.13 | 21  | Tested    |

## Support window policy

- We support Spark `3.5.x` and `4.0.x` at the same time on the current release line.
- New patch releases keep support for both Spark lines unless a security or upstream break forces a change.
- If a Spark line is dropped, it is announced in release notes before the drop.

## Platform and matrix coverage

| Area                           | Status                |
|--------------------------------|-----------------------|
| Spark integration module Scala | 2.13                  |
| Root project Scala             | 2.13 and 3.3.3        |
| CI operating systems           | Linux, macOS, Windows |
| CI JDK                         | 21                    |
| Spark compatibility checks     | 3.5.0 and 4.0.1       |

## Notes

- Published spark-integration artifact compiles against Spark 3.5.0 APIs.
- CI validates runtime compatibility on Spark 4.0.1.
- Windows DataSource V2 write tests depend on Hadoop `winutils` and are guarded in tests.
- Spark integration is intentionally Scala 2.13 because Spark runtime APIs are Scala 2 based.

Trademark notice: Apache Spark and Spark are trademarks of The Apache Software Foundation.
