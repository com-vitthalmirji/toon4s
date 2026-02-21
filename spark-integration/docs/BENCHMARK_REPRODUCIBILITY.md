# Benchmark reproducibility

This page only describes how to reproduce benchmark numbers locally.

## Environment to record

- OS and version
- CPU model
- RAM
- JDK version
- sbt version
- Spark version

## Core JMH benchmark

Run from repository root:

```bash
sbt "jmh/jmh:run -i 5 -wi 5 -f1 -t1 io.toonformat.toon4s.jmh.EncodeDecodeBench"
```

Run one benchmark method:

```bash
sbt "jmh/jmh:run -i 5 -wi 5 -f1 -t1 io.toonformat.toon4s.jmh.EncodeDecodeBench.encode_real_world"
```

## Spark integration benchmark style run

Use a representative DataFrame and call:

```scala
import io.toonformat.toon4s.spark.monitoring.ToonMonitoring._

measureEncodingPerformance(df, key = "data", chunkSize = 1000)
```

Record:

- row count and column count
- elapsed time
- rows per second
- JSON token count
- TOON token count
- savings percent

## Rules for claim updates

- Do not update public benchmark claims without rerunning benchmarks.
- Keep command lines and environment details with results.

Trademark notice: Apache Spark and Spark are trademarks of The Apache Software Foundation.
